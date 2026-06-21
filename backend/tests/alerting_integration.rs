//! Tests d'intégration du moteur d'alerting :
//! - Slack mocké (serveur axum local) recevant le webhook ;
//! - `RecordingNotifier` partagé pour vérifier le contenu de l'alerte et le cooldown ;
//! - construction du `Message` e-mail (sans envoi SMTP réel).

mod common;

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde_json::Value;
use sqlx::PgPool;

use datacat_ingest::alerting::{
    evaluate_once, parse_rules, AlertEngineState, AlertState, DispatchSettings, Dispatcher,
    Notifier, RecordingNotifier, SlackNotifier,
};

/// Garantit l'existence des partitions de logs / métriques autour d'aujourd'hui.
/// (`#[sqlx::test]` applique les migrations mais ne crée pas les partitions.)
async fn ensure_partitions(pool: &PgPool) {
    datacat_ingest::db::ensure_log_partition_window(pool, 2, 2)
        .await
        .unwrap();
    datacat_ingest::db::ensure_metric_partition_window(pool, 2, 2)
        .await
        .unwrap();
    datacat_ingest::db::ensure_span_partition_window(pool, 2, 2)
        .await
        .unwrap();
}

/// Insère un point de métrique gauge (value_double) à `now`.
async fn insert_metric(pool: &PgPool, name: &str, service: &str, value: f64) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO metric_points \
         (point_id, time, metric_name, metric_type, service_name, value_double, received_at, \
          resource_attributes, attributes) \
         VALUES (gen_random_uuid(), $1, $2, 'gauge', $3, $4, now(), '{}'::jsonb, '{}'::jsonb)",
    )
    .bind(now)
    .bind(name)
    .bind(service)
    .bind(value)
    .execute(pool)
    .await
    .unwrap();
}

/// Insère un log ERROR (severity 17) à `now` pour un service / corps donnés.
async fn insert_log_body(pool: &PgPool, service: &str, body: &str) {
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO logs \
         (log_id, log_time, received_at, severity_number, severity_text, body, service_name, \
          resource_attributes, log_attributes) \
         VALUES (gen_random_uuid(), $1, now(), 17, 'ERROR', $2, $3, '{}'::jsonb, '{}'::jsonb)",
    )
    .bind(now)
    .bind(body)
    .bind(service)
    .execute(pool)
    .await
    .unwrap();
}

/// Insère un log ERROR (severity 17, corps `boom`) à `now` pour un service donné.
async fn insert_error_log(pool: &PgPool, service: &str) {
    insert_log_body(pool, service, "boom").await;
}

/// Insère un log de sévérité arbitraire à un instant donné (pour ratio / fenêtres relatives).
async fn insert_log_at(
    pool: &PgPool,
    service: &str,
    severity: i16,
    log_time: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO logs \
         (log_id, log_time, received_at, severity_number, severity_text, body, service_name, \
          resource_attributes, log_attributes) \
         VALUES (gen_random_uuid(), $1, now(), $2, 'X', 'msg', $3, '{}'::jsonb, '{}'::jsonb)",
    )
    .bind(log_time)
    .bind(severity)
    .bind(service)
    .execute(pool)
    .await
    .unwrap();
}

/// Insère un span (latence + status) à `start_time` pour un service / opération donnés.
async fn insert_span(
    pool: &PgPool,
    service: &str,
    name: &str,
    duration_ms: f64,
    status_code: i16,
    start_time: chrono::DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO spans \
         (trace_id, span_id, start_time, duration_ms, name, service_name, status_code, \
          received_at, resource_attributes, span_attributes) \
         VALUES (gen_random_uuid()::text, gen_random_uuid()::text, $1, $2, $3, $4, $5, now(), \
                 '{}'::jsonb, '{}'::jsonb)",
    )
    .bind(start_time)
    .bind(duration_ms)
    .bind(name)
    .bind(service)
    .bind(status_code)
    .execute(pool)
    .await
    .unwrap();
}

// ── Mock Slack (serveur axum local capturant le webhook) ──────────────────────

#[derive(Clone, Default)]
struct SlackMockState {
    received: Arc<Mutex<Vec<Value>>>,
}

async fn slack_mock_handler(
    State(state): State<SlackMockState>,
    Json(body): Json<Value>,
) -> &'static str {
    state.received.lock().unwrap().push(body);
    "ok"
}

/// Démarre un mock Slack ; retourne (url du webhook, payloads reçus).
async fn start_slack_mock() -> (String, Arc<Mutex<Vec<Value>>>) {
    let state = SlackMockState::default();
    let received = Arc::clone(&state.received);
    let app = Router::new()
        .route("/webhook", post(slack_mock_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/webhook"), received)
}

#[sqlx::test]
async fn metric_threshold_fires_slack(pool: PgPool) {
    ensure_partitions(&pool).await;
    // Seed : moyenne (avg) au-dessus du seuil 500.
    insert_metric(&pool, "http.server.duration", "api", 700.0).await;
    insert_metric(&pool, "http.server.duration", "api", 900.0).await;

    let (webhook_url, received) = start_slack_mock().await;

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"latence", "kind":"metric_threshold", "metric_name":"http.server.duration",
              "service":"api", "agg":"avg", "window_secs":300, "comparator":"gt",
              "threshold":500, "cooldown_secs":600, "severity":"critical" }
        ] }"#,
    )
    .unwrap();

    let notifiers: Vec<Arc<dyn Notifier>> = vec![Arc::new(SlackNotifier::new(webhook_url))];
    let dispatcher = Dispatcher::with_defaults(notifiers);
    let mut state = AlertEngineState::new();

    let notified = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(notified, 1, "une alerte notifiée à la transition ok→firing");

    let payloads = received.lock().unwrap();
    assert_eq!(
        payloads.len(),
        1,
        "le mock Slack a reçu exactement un webhook"
    );
    let text = payloads[0]["text"].as_str().unwrap();
    assert!(text.contains("[FIRING]"), "{text}");
    assert!(text.contains("latence"), "{text}");
    assert!(text.contains("critical"), "{text}");
}

#[sqlx::test]
async fn log_count_fires_and_respects_cooldown(pool: PgPool) {
    ensure_partitions(&pool).await;
    // Seed : 3 logs ERROR du service billing → > seuil 2.
    for _ in 0..3 {
        insert_error_log(&pool, "billing").await;
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"erreurs billing", "kind":"log_count", "service":"billing",
              "severity_min":17, "window_secs":300, "comparator":"gt", "threshold":2,
              "cooldown_secs":600, "severity":"critical" }
        ] }"#,
    )
    .unwrap();

    let recorder = RecordingNotifier::new();
    let dispatcher =
        Dispatcher::with_defaults(vec![Arc::new(recorder.clone()) as Arc<dyn Notifier>]);
    let mut state = AlertEngineState::new();

    // 1re évaluation : transition ok→firing → une alerte.
    let now = Utc::now();
    let n1 = evaluate_once(&pool, &rules, &mut state, &dispatcher, now).await;
    assert_eq!(n1, 1);

    // 2e évaluation immédiate (même état firing, dans le cooldown) → aucune re-notification.
    let n2 = evaluate_once(&pool, &rules, &mut state, &dispatcher, now).await;
    assert_eq!(n2, 0, "le cooldown empêche la re-notification");

    let alerts = recorder.alerts();
    assert_eq!(alerts.len(), 1, "une seule alerte malgré deux évaluations");
    let a = &alerts[0];
    assert_eq!(a.rule_name, "erreurs billing");
    assert_eq!(a.severity, "critical");
    assert_eq!(a.state, AlertState::Firing);
    assert_eq!(a.value, 3.0, "compte de logs ERROR = 3");
    assert_eq!(a.threshold, 2.0);
}

#[sqlx::test]
async fn no_alert_when_below_threshold(pool: PgPool) {
    ensure_partitions(&pool).await;
    // Une seule erreur, seuil à 2 → pas de déclenchement.
    insert_error_log(&pool, "billing").await;

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"erreurs billing", "kind":"log_count", "service":"billing",
              "severity_min":17, "window_secs":300, "comparator":"gt", "threshold":2,
              "cooldown_secs":0 }
        ] }"#,
    )
    .unwrap();

    let recorder = RecordingNotifier::new();
    let dispatcher =
        Dispatcher::with_defaults(vec![Arc::new(recorder.clone()) as Arc<dyn Notifier>]);
    let mut state = AlertEngineState::new();

    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n, 0);
    assert!(recorder.alerts().is_empty());
}

#[sqlx::test]
async fn fires_then_resolves(pool: PgPool) {
    ensure_partitions(&pool).await;
    insert_error_log(&pool, "billing").await;
    insert_error_log(&pool, "billing").await;
    insert_error_log(&pool, "billing").await;

    // cooldown 0 → on peut notifier la résolution immédiatement après le firing.
    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"erreurs billing", "kind":"log_count", "service":"billing",
              "severity_min":17, "window_secs":300, "comparator":"gt", "threshold":2,
              "cooldown_secs":0 }
        ] }"#,
    )
    .unwrap();

    let recorder = RecordingNotifier::new();
    let dispatcher =
        Dispatcher::with_defaults(vec![Arc::new(recorder.clone()) as Arc<dyn Notifier>]);
    let mut state = AlertEngineState::new();

    // Firing (3 > 2).
    let n1 = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n1, 1);

    // On purge les logs : le compte retombe à 0 → transition firing→ok = résolu.
    sqlx::query("DELETE FROM logs")
        .execute(&pool)
        .await
        .unwrap();
    let n2 = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n2, 1, "résolution notifiée");

    let alerts = recorder.alerts();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].state, AlertState::Firing);
    assert_eq!(alerts[1].state, AlertState::Resolved);
}

// ── Mock webhook générique (capture corps + en-tête custom) ───────────────────

/// Capture partagée : `(en-tête x-test, corps JSON)` de chaque requête reçue.
type WebhookCapture = Arc<Mutex<Vec<(Option<String>, Value)>>>;

#[derive(Clone, Default)]
struct WebhookMockState {
    received: WebhookCapture,
}

async fn webhook_mock_handler(
    State(state): State<WebhookMockState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> &'static str {
    let h = headers
        .get("x-test")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    state.received.lock().unwrap().push((h, body));
    "ok"
}

async fn start_webhook_mock() -> (String, WebhookCapture) {
    let state = WebhookMockState::default();
    let received = Arc::clone(&state.received);
    let app = Router::new()
        .route("/hook", post(webhook_mock_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/hook"), received)
}

/// « 5 erreurs identiques → webhook », regroupées par `body`. Deux messages distincts atteignant
/// le seuil déclenchent deux webhooks indépendants (un par groupe), avec en-tête custom. Aucun
/// notifier global : seule l'action de la règle est utilisée.
#[sqlx::test]
async fn log_group_count_webhook_per_group(pool: PgPool) {
    ensure_partitions(&pool).await;
    for _ in 0..5 {
        insert_log_body(&pool, "billing", "boom").await;
    }
    for _ in 0..5 {
        insert_log_body(&pool, "billing", "kaboom").await;
    }
    // Un 3e message sous le seuil → ne déclenche pas.
    insert_log_body(&pool, "billing", "blip").await;

    let (hook_url, received) = start_webhook_mock().await;
    let raw = format!(
        r#"{{ "rules": [
            {{ "name":"erreurs identiques", "kind":"log_group_count", "service":"billing",
               "severity_min":17, "window_secs":300, "comparator":"gte", "threshold":5,
               "cooldown_secs":0, "group_by":"body",
               "actions":[ {{ "type":"webhook", "url":"{hook_url}", "headers": {{ "x-test":"1" }} }} ] }}
        ] }}"#
    );
    let rules = parse_rules(&raw).unwrap();

    let dispatcher = Dispatcher::build(&rules, &DispatchSettings::default(), vec![]);
    let mut state = AlertEngineState::new();

    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n, 2, "deux groupes (boom, kaboom) franchissent le seuil");

    let payloads = received.lock().unwrap();
    assert_eq!(payloads.len(), 2, "un webhook par groupe en alerte");
    let mut groups: Vec<String> = payloads
        .iter()
        .map(|(h, body)| {
            assert_eq!(h.as_deref(), Some("1"), "en-tête custom transmis");
            assert_eq!(body["state"], "FIRING");
            assert_eq!(body["rule"], "erreurs identiques");
            body["group_key"].as_str().unwrap().to_string()
        })
        .collect();
    groups.sort();
    assert_eq!(groups, vec!["boom".to_string(), "kaboom".to_string()]);
}

/// Le repli : une règle `log_group_count` sans `actions` utilise les notifiers par défaut, et
/// chaque groupe porte sa `group_key`.
#[sqlx::test]
async fn log_group_count_uses_default_notifier(pool: PgPool) {
    ensure_partitions(&pool).await;
    for _ in 0..6 {
        insert_log_body(&pool, "api", "db timeout").await;
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"erreurs groupées", "kind":"log_group_count", "service":"api",
              "severity_min":17, "window_secs":300, "comparator":"gte", "threshold":5,
              "cooldown_secs":0, "group_by":"body" }
        ] }"#,
    )
    .unwrap();

    let recorder = RecordingNotifier::new();
    let dispatcher =
        Dispatcher::with_defaults(vec![Arc::new(recorder.clone()) as Arc<dyn Notifier>]);
    let mut state = AlertEngineState::new();

    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n, 1);
    let alerts = recorder.alerts();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].group_key.as_deref(), Some("db timeout"));
    assert_eq!(alerts[0].value, 6.0);
    assert_eq!(alerts[0].state, AlertState::Firing);
}

// ── Cas d'usage standard : taux d'erreur, heartbeat, latence p95, pic relatif ─

/// Petit utilitaire : un dispatcher enregistreur + un état neuf.
fn recorder_setup() -> (RecordingNotifier, Dispatcher, AlertEngineState) {
    let recorder = RecordingNotifier::new();
    let dispatcher =
        Dispatcher::with_defaults(vec![Arc::new(recorder.clone()) as Arc<dyn Notifier>]);
    (recorder, dispatcher, AlertEngineState::new())
}

/// `error_ratio` (logs) : 2 erreurs / 10 logs = 0.2 > seuil 0.1 ⇒ déclenche. Une seconde règle au
/// même seuil mais `min_count` élevé ne déclenche pas (échantillon trop petit).
#[sqlx::test]
async fn error_ratio_logs_fires_and_min_count_guards(pool: PgPool) {
    ensure_partitions(&pool).await;
    let now = Utc::now();
    for _ in 0..2 {
        insert_log_at(&pool, "api", 17, now).await; // erreurs
    }
    for _ in 0..8 {
        insert_log_at(&pool, "api", 9, now).await; // infos
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"taux erreur api", "kind":"error_ratio", "source":"logs", "service":"api",
              "severity_min":17, "min_count":5, "window_secs":300, "comparator":"gt",
              "threshold":0.1, "cooldown_secs":0 },
            { "name":"taux erreur (gros echantillon requis)", "kind":"error_ratio", "source":"logs",
              "service":"api", "severity_min":17, "min_count":100, "window_secs":300,
              "comparator":"gt", "threshold":0.1, "cooldown_secs":0 }
        ] }"#,
    )
    .unwrap();

    let (recorder, dispatcher, mut state) = recorder_setup();
    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, now).await;
    assert_eq!(n, 1, "seule la règle au min_count raisonnable déclenche");
    let alerts = recorder.alerts();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].rule_name, "taux erreur api");
    assert!(
        (alerts[0].value - 0.2).abs() < 1e-9,
        "ratio = {}",
        alerts[0].value
    );
}

/// `telemetry_count` en mode heartbeat (no-data) : `lte 0` sur les métriques d'un service. La base
/// vide déclenche (0 ≤ 0) ; l'arrivée d'un point résout.
#[sqlx::test]
async fn heartbeat_no_data_then_resolves(pool: PgPool) {
    ensure_partitions(&pool).await;
    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"heartbeat api", "kind":"telemetry_count", "source":"metrics",
              "service":"api", "window_secs":300, "comparator":"lte", "threshold":0,
              "cooldown_secs":0, "severity":"critical" }
        ] }"#,
    )
    .unwrap();

    let (recorder, dispatcher, mut state) = recorder_setup();

    // Aucune métrique → no-data → alerte FIRING.
    let n1 = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n1, 1);
    assert_eq!(recorder.alerts()[0].state, AlertState::Firing);

    // Un point arrive → la télémétrie reprend → RESOLVED.
    insert_metric(&pool, "up", "api", 1.0).await;
    let n2 = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n2, 1);
    let alerts = recorder.alerts();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[1].state, AlertState::Resolved);
}

/// `span_duration` p95 : des spans lents font franchir le seuil de latence (en ms).
#[sqlx::test]
async fn span_duration_p95_fires(pool: PgPool) {
    ensure_partitions(&pool).await;
    let now = Utc::now();
    for _ in 0..18 {
        insert_span(&pool, "api", "checkout", 100.0, 1, now).await;
    }
    for _ in 0..2 {
        insert_span(&pool, "api", "checkout", 5000.0, 1, now).await;
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"p95 checkout", "kind":"span_duration", "agg":"p95", "service":"api",
              "operation":"checkout", "window_secs":300, "comparator":"gt", "threshold":1000,
              "cooldown_secs":0, "severity":"warning" }
        ] }"#,
    )
    .unwrap();

    let (recorder, dispatcher, mut state) = recorder_setup();
    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, now).await;
    assert_eq!(n, 1);
    let alerts = recorder.alerts();
    assert!(
        alerts[0].value > 1000.0,
        "p95 attendu élevé, obtenu {}",
        alerts[0].value
    );
}

/// `relative_change` : 10 logs sur la fenêtre courante vs 2 sur la précédente ⇒ ratio 5 > 3.
#[sqlx::test]
async fn relative_change_spike_fires(pool: PgPool) {
    ensure_partitions(&pool).await;
    let now = Utc::now();
    // Fenêtre précédente [-600s, -300s] : 2 logs (vers -450s).
    for _ in 0..2 {
        insert_log_at(&pool, "api", 17, now - chrono::Duration::seconds(450)).await;
    }
    // Fenêtre courante [-300s, now] : 10 logs (à now).
    for _ in 0..10 {
        insert_log_at(&pool, "api", 17, now).await;
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"pic erreurs api", "kind":"relative_change", "source":"logs", "service":"api",
              "severity_min":17, "window_secs":300, "comparator":"gt", "threshold":3,
              "cooldown_secs":0, "severity":"warning" }
        ] }"#,
    )
    .unwrap();

    let (recorder, dispatcher, mut state) = recorder_setup();
    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, now).await;
    assert_eq!(n, 1);
    assert!(
        (recorder.alerts()[0].value - 5.0).abs() < 1e-9,
        "ratio = {}",
        recorder.alerts()[0].value
    );
}

/// `metric_threshold` avec agrégat percentile (`p95`) sur les points de métrique.
#[sqlx::test]
async fn metric_threshold_p95_fires(pool: PgPool) {
    ensure_partitions(&pool).await;
    for _ in 0..18 {
        insert_metric(&pool, "http.server.duration", "api", 100.0).await;
    }
    for _ in 0..2 {
        insert_metric(&pool, "http.server.duration", "api", 4000.0).await;
    }

    let rules = parse_rules(
        r#"{ "rules": [
            { "name":"p95 latence", "kind":"metric_threshold", "metric_name":"http.server.duration",
              "service":"api", "agg":"p95", "window_secs":300, "comparator":"gt", "threshold":1000,
              "cooldown_secs":0, "severity":"warning" }
        ] }"#,
    )
    .unwrap();

    let (recorder, dispatcher, mut state) = recorder_setup();
    let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
    assert_eq!(n, 1, "p95 au-dessus du seuil");
    assert!(recorder.alerts()[0].value > 1000.0);
}
