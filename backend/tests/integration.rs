//! Tests d'intégration end-to-end (HTTP → Axum → PostgreSQL réel).
//!
//! Chaque test obtient une base isolée via `#[sqlx::test]` (migrations appliquées
//! automatiquement). Nécessite un PostgreSQL joignable via `DATABASE_URL`.

mod common;

use std::time::Duration;

use chrono::Utc;
use common::*;
use jsonwebtoken::Algorithm;
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Idempotence (critère d'acceptation §12)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn same_event_id_counts_once(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);

    let id = Uuid::new_v4();
    let ts = Utc::now();
    let ev = event_json(id, "validate_planning", "sess-1", ts);

    // Doublon DANS le même batch + 3 renvois identiques (retry réseau simulé).
    for _ in 0..3 {
        let resp = client
            .post(format!("{}/v1/events", app.base_url))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "events": [ev, ev] }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202, "le POST doit être acquitté");
    }

    app.wait_total(1, Duration::from_secs(5)).await;
    assert_eq!(app.count_event_id(id).await, 1, "un seul event_id stocké");
    assert_eq!(app.count_events().await, 1, "aucun doublon");
}

// ─────────────────────────────────────────────────────────────────────────────
// Écriture COPY : plusieurs events distincts persistés correctement
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn copy_persists_distinct_events(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);

    let ts = Utc::now();
    let events: Vec<_> = (0..50)
        .map(|i| event_json(Uuid::new_v4(), &format!("event_{i}"), "sess-1", ts))
        .collect();

    let resp = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": events }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["received"], 50);

    assert_eq!(app.wait_total(50, Duration::from_secs(5)).await, 50);

    // received_at renseigné par le serveur, properties conservées.
    let row: (String, serde_json::Value, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
        "SELECT event_name, properties, received_at FROM events ORDER BY event_name LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.0, "event_0");
    assert_eq!(row.1, serde_json::json!({ "k": "v" }));
    assert!(row.2.is_some());
}

// ─────────────────────────────────────────────────────────────────────────────
// Vérification du token (critère §12)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn token_is_required_and_verified(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let ev = event_json(Uuid::new_v4(), "click", "sess-1", Utc::now());
    let body = serde_json::json!({ "events": [ev] });

    // Sans token → 401.
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "token requis");

    // Token bidon → 401.
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth("pas.un.jwt")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // Token expiré → 401.
    let expired = mint_ed("actor-1", "sess-1", -10);
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&expired)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "token expiré rejeté");

    // Token valide → 202.
    let valid = mint_ed("actor-1", "sess-1", 600);
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&valid)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "token valide accepté");
}

#[sqlx::test]
async fn token_in_body_works_for_beacon(pool: PgPool) {
    // Repli sendBeacon : token dans le corps (CONTRACT §1.1), sans en-tête Authorization.
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);
    let ev = event_json(Uuid::new_v4(), "page_unload", "sess-1", Utc::now());

    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .json(&serde_json::json!({ "token": token, "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(app.wait_total(1, Duration::from_secs(5)).await, 1);
}

#[sqlx::test]
async fn rs256_token_accepted(pool: PgPool) {
    // Variante RS256 : clé publique RSA côté ingestion, token signé RS256.
    let mut token_cfg = token_enabled_ed();
    token_cfg.key_source = Some(datacat_ingest::config::KeySource::Pem {
        pem: RSA_PUBLIC.to_string(),
        alg: Algorithm::RS256,
        kid: None,
    });
    let app = start_app(pool, test_config(token_cfg, |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint(
        RSA_PRIVATE.as_bytes(),
        Algorithm::RS256,
        "actor-1",
        "sess-1",
        None,
        600,
    );
    let ev = event_json(Uuid::new_v4(), "click", "sess-1", Utc::now());

    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "token RS256 valide accepté");
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation stricte
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn rejects_invalid_and_oversized(pool: PgPool) {
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| c.limits.max_batch_events = 5),
    )
    .await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);
    let post = |body: serde_json::Value| {
        let c = client.clone();
        let url = format!("{}/v1/events", app.base_url);
        let t = token.clone();
        async move { c.post(url).bearer_auth(t).json(&body).send().await.unwrap() }
    };

    // Batch vide → 400.
    assert_eq!(
        post(serde_json::json!({ "events": [] })).await.status(),
        400
    );

    // Champ requis manquant (actor_id) → 400.
    let bad = serde_json::json!({ "events": [{
        "event_id": Uuid::new_v4(), "event_name": "x",
        "session_id": "s", "timestamp_client": Utc::now().to_rfc3339()
    }]});
    assert_eq!(post(bad).await.status(), 400);

    // event_name vide → 400.
    let empty_name = serde_json::json!({ "events": [
        event_json_named(Uuid::new_v4(), "   ", "sess-1")
    ]});
    assert_eq!(post(empty_name).await.status(), 400);

    // Batch trop grand (6 > 5) → 413.
    let big: Vec<_> = (0..6)
        .map(|i| event_json(Uuid::new_v4(), &format!("e{i}"), "sess-1", Utc::now()))
        .collect();
    assert_eq!(
        post(serde_json::json!({ "events": big })).await.status(),
        413
    );
}

fn event_json_named(id: Uuid, name: &str, session: &str) -> serde_json::Value {
    serde_json::json!({
        "event_id": id, "event_name": name, "actor_id": "actor-1",
        "session_id": session, "timestamp_client": Utc::now().to_rfc3339()
    })
}

#[sqlx::test]
async fn out_of_skew_event_dropped_not_rejected(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);

    // Un event hors fenêtre (40 j) + un event valide dans le même batch.
    let old = event_json(
        Uuid::new_v4(),
        "old",
        "sess-1",
        Utc::now() - chrono::Duration::days(40),
    );
    let ok = event_json(Uuid::new_v4(), "fresh", "sess-1", Utc::now());

    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": [old, ok] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["received"], 1, "seul l'event valide est retenu");

    assert_eq!(app.wait_total(1, Duration::from_secs(5)).await, 1);
    // L'event hors fenêtre est compté comme écarté (perte tolérée), pas rejeté.
    assert_eq!(
        app.metrics
            .dropped_skew_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Rate limiting end-to-end (critère §12)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn session_rate_limit_isolates_sessions(pool: PgPool) {
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| {
            c.rate_limit.session_per_sec = 1.0;
            c.rate_limit.session_burst = 5.0;
        }),
    )
    .await;
    let client = reqwest::Client::new();

    // Session abusive : un batch de 6 events > burst 5 → 429 scope session.
    let token_a = mint_ed("actor-1", "sess-abuser", 600);
    let big: Vec<_> = (0..6)
        .map(|i| event_json(Uuid::new_v4(), &format!("e{i}"), "sess-abuser", Utc::now()))
        .collect();
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token_a)
        .json(&serde_json::json!({ "events": big }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    assert!(r.headers().contains_key("retry-after"));
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["scope"], "session");

    // Un collègue (autre session, même IP) n'est pas impacté.
    let token_b = mint_ed("actor-2", "sess-colleague", 600);
    let ev = event_json(Uuid::new_v4(), "click", "sess-colleague", Utc::now());
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token_b)
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202, "session du collègue non impactée");
}

#[sqlx::test]
async fn ip_session_cap_blocks_fake_session_flood(pool: PgPool) {
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| c.rate_limit.ip_session_cap = 3),
    )
    .await;
    let client = reqwest::Client::new();

    // 3 sessions distinctes depuis la même IP (127.0.0.1) → OK.
    for i in 0..3 {
        let token = mint_ed("actor", &format!("sess-{i}"), 600);
        let ev = event_json(Uuid::new_v4(), "click", &format!("sess-{i}"), Utc::now());
        let r = client
            .post(format!("{}/v1/events", app.base_url))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "events": [ev] }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
    }
    // La 4e session distincte depuis la même IP → 429 scope ip_sessions.
    let token = mint_ed("actor", "sess-flood", 600);
    let ev = event_json(Uuid::new_v4(), "click", "sess-flood", Utc::now());
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["scope"], "ip_sessions");
}

// ─────────────────────────────────────────────────────────────────────────────
// DROP PARTITION (purge de rétention) (critère §12)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn purge_drops_old_partition(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("actor-1", "sess-1", 600);

    // Event daté de 10 jours (dans la fenêtre de skew 31 j).
    let ts = Utc::now() - chrono::Duration::days(10);
    let ev = event_json(Uuid::new_v4(), "old_action", "sess-1", ts);
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(app.wait_total(1, Duration::from_secs(5)).await, 1);

    let part = format!("events_p{}", ts.format("%Y%m%d"));
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(&part)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert!(exists, "la partition {part} doit exister");

    // Purge avec rétention 5 jours → DROP de la partition de 10 jours.
    let dropped = datacat_ingest::db::purge_old_partitions(&app.pool, 5)
        .await
        .unwrap();
    assert!(dropped >= 1, "au moins une partition supprimée");

    let exists_after: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)")
            .bind(&part)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert!(
        !exists_after,
        "la partition doit être supprimée (DROP PARTITION)"
    );
    assert_eq!(
        app.count_events().await,
        0,
        "events purgés avec la partition"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Pic d'écriture : pas de perte (sous tolérance) et AUCUN doublon (critère §12)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn write_spike_no_duplicates(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("actor-1", "sess-spike", 600);

    const UNIQUE: usize = 2_000;
    const BATCH: usize = 100;
    let ts = Utc::now();

    // Pré-génère les ids uniques, répartis en batches.
    let ids: Vec<Uuid> = (0..UNIQUE).map(|_| Uuid::new_v4()).collect();
    let batches: Vec<Vec<serde_json::Value>> = ids
        .chunks(BATCH)
        .map(|chunk| {
            chunk
                .iter()
                .map(|id| event_json(*id, "spike", "sess-spike", ts))
                .collect()
        })
        .collect();

    // Chaque batch est envoyé DEUX fois (doublons) et en concurrence.
    let mut handles = Vec::new();
    for batch in &batches {
        for _ in 0..2 {
            let url = format!("{}/v1/events", app.base_url);
            let t = token.clone();
            let body = serde_json::json!({ "events": batch });
            handles.push(tokio::spawn(async move {
                reqwest::Client::new()
                    .post(url)
                    .bearer_auth(t)
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .as_u16()
            }));
        }
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), 202);
    }

    let total = app.wait_total(UNIQUE as i64, Duration::from_secs(20)).await;
    assert_eq!(
        total, UNIQUE as i64,
        "aucune perte et aucun doublon malgré 2x envois"
    );

    let distinct: i64 = sqlx::query_scalar("SELECT count(DISTINCT event_id) FROM events")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(distinct, UNIQUE as i64);
}

// ─────────────────────────────────────────────────────────────────────────────
// Mode token désactivé (dev local)
// ─────────────────────────────────────────────────────────────────────────────

#[sqlx::test]
async fn works_with_token_disabled(pool: PgPool) {
    let app = start_app(pool, test_config(token_disabled(), |_| {})).await;
    let client = reqwest::Client::new();
    let ev = event_json(Uuid::new_v4(), "click", "sess-1", Utc::now());
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(app.wait_total(1, Duration::from_secs(5)).await, 1);
    app.shutdown().await;
}
