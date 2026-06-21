//! Évaluateur d'alertes : tâche de fond périodique + fonction `evaluate_once` testable.
//!
//! Pour chaque règle, une requête SQL calcule la valeur courante sur la fenêtre, comparée au
//! seuil. Une machine à états par règle (ok ↔ firing) avec **cooldown** garantit qu'on ne
//! notifie qu'aux transitions (ok→firing, et firing→ok = résolu), et au plus une fois par
//! `cooldown_secs`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};

use crate::alerting::notify::{Alert, AlertState, Dispatcher, Notifier};
use crate::alerting::rules::{Agg, GroupExpr, Rule, RuleKind, Source};

/// État runtime d'une règle (machine à états + horodatage de la dernière notification).
#[derive(Debug, Default, Clone)]
pub struct RuleState {
    /// La règle est-elle actuellement en état « firing » ?
    pub firing: bool,
    /// Dernière notification émise (pour le respect du cooldown).
    pub last_notified: Option<DateTime<Utc>>,
}

/// État de toutes les règles, indexé par nom de règle.
pub type AlertEngineState = HashMap<String, RuleState>;

/// Évalue toutes les règles une fois et notifie les transitions (en respectant le cooldown).
/// `now` est injecté pour la testabilité (fenêtres + cooldown déterministes).
/// Les notifiers déclenchés sont résolus par règle via le [`Dispatcher`] (actions modulables).
/// Retourne le nombre d'alertes effectivement notifiées.
pub async fn evaluate_once(
    pool: &PgPool,
    rules: &[Rule],
    state: &mut AlertEngineState,
    dispatcher: &Dispatcher,
    now: DateTime<Utc>,
) -> usize {
    let mut notified = 0;
    for rule in rules {
        let notifiers = dispatcher.for_rule(rule);
        notified += match rule.kind {
            RuleKind::LogGroupCount => evaluate_group_rule(pool, rule, state, notifiers, now).await,
            _ => evaluate_scalar_rule(pool, rule, state, notifiers, now).await,
        };
    }
    notified
}

/// Règle scalaire (`log_count` / `metric_threshold`) : une valeur, un état.
async fn evaluate_scalar_rule(
    pool: &PgPool,
    rule: &Rule,
    state: &mut AlertEngineState,
    notifiers: &[Arc<dyn Notifier>],
    now: DateTime<Utc>,
) -> usize {
    let value = match compute_value(pool, rule, now).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(rule = %rule.name, error = %e, "évaluation de la règle échouée");
            return 0;
        }
    };
    let breaching = rule.comparator.compare(value, rule.threshold);
    usize::from(
        process_transition(
            state, &rule.name, breaching, rule, value, None, notifiers, now,
        )
        .await,
    )
}

/// Règle `log_group_count` : un état **par groupe** (`rule::group_key`). Chaque groupe dont le
/// compte franchit le seuil déclenche indépendamment ; un groupe précédemment en alerte mais
/// absent de la fenêtre est résolu (compte 0).
async fn evaluate_group_rule(
    pool: &PgPool,
    rule: &Rule,
    state: &mut AlertEngineState,
    notifiers: &[Arc<dyn Notifier>],
    now: DateTime<Utc>,
) -> usize {
    let groups = match compute_groups(pool, rule, now).await {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(rule = %rule.name, error = %e, "évaluation (groupes) de la règle échouée");
            return 0;
        }
    };

    let mut notified = 0;
    let mut seen: HashSet<String> = HashSet::new();
    for (group_key, count) in groups {
        seen.insert(group_key.clone());
        let breaching = rule.comparator.compare(count, rule.threshold);
        let state_key = format!("{}::{}", rule.name, group_key);
        if process_transition(
            state,
            &state_key,
            breaching,
            rule,
            count,
            Some(group_key),
            notifiers,
            now,
        )
        .await
        {
            notified += 1;
        }
    }

    // Groupes précédemment en alerte mais disparus de la fenêtre → résolus (compte 0).
    let prefix = format!("{}::", rule.name);
    let stale: Vec<(String, String)> = state
        .iter()
        .filter(|(k, v)| v.firing && k.starts_with(&prefix) && !seen.contains(&k[prefix.len()..]))
        .map(|(k, _)| (k.clone(), k[prefix.len()..].to_string()))
        .collect();
    for (state_key, group_key) in stale {
        if process_transition(
            state,
            &state_key,
            false,
            rule,
            0.0,
            Some(group_key),
            notifiers,
            now,
        )
        .await
        {
            notified += 1;
        }
    }
    notified
}

/// Applique la machine à états (ok ↔ firing) à une clé d'état donnée et notifie la transition
/// éventuelle (en respectant le cooldown). Retourne `true` si une alerte a été notifiée.
#[allow(clippy::too_many_arguments)]
async fn process_transition(
    state: &mut AlertEngineState,
    state_key: &str,
    breaching: bool,
    rule: &Rule,
    value: f64,
    group_key: Option<String>,
    notifiers: &[Arc<dyn Notifier>],
    now: DateTime<Utc>,
) -> bool {
    let entry = state.entry(state_key.to_string()).or_default();

    // Transition à notifier ?
    let transition = match (entry.firing, breaching) {
        (false, true) => Some(AlertState::Firing),
        (true, false) => Some(AlertState::Resolved),
        _ => None,
    };
    entry.firing = breaching;

    let Some(alert_state) = transition else {
        return false;
    };

    // Cooldown : pas plus d'une notification par `cooldown_secs` pour cette clé d'état.
    if let Some(last) = entry.last_notified {
        let elapsed = (now - last).to_std().unwrap_or(Duration::ZERO);
        if elapsed < Duration::from_secs(rule.cooldown_secs) {
            return false;
        }
    }

    let alert = Alert {
        rule_name: rule.name.clone(),
        severity: rule.severity.clone(),
        state: alert_state,
        value,
        threshold: rule.threshold,
        description: describe(rule),
        group_key,
    };
    let mut any_sent = false;
    for n in notifiers {
        match n.send(&alert).await {
            Ok(()) => any_sent = true,
            Err(e) => {
                tracing::warn!(rule = %rule.name, error = %e, "envoi de notification échoué")
            }
        }
    }
    if any_sent {
        entry.last_notified = Some(now);
    }
    any_sent
}

/// Description lisible de la condition d'une règle.
fn describe(rule: &Rule) -> String {
    match rule.kind {
        RuleKind::LogCount => {
            let svc = rule.service.as_deref().unwrap_or("*");
            let sev = rule
                .severity_min
                .map(|s| format!(", severity>={s}"))
                .unwrap_or_default();
            format!(
                "count(logs service={svc}{sev}) {} {} over {}s",
                rule.comparator.symbol(),
                rule.threshold,
                rule.window_secs
            )
        }
        RuleKind::LogGroupCount => {
            let svc = rule.service.as_deref().unwrap_or("*");
            let sev = rule
                .severity_min
                .map(|s| format!(", severity>={s}"))
                .unwrap_or_default();
            let by = rule.group_by.as_deref().unwrap_or("body");
            format!(
                "count(logs service={svc}{sev}) by {by} {} {} over {}s",
                rule.comparator.symbol(),
                rule.threshold,
                rule.window_secs
            )
        }
        RuleKind::MetricThreshold => {
            let metric = rule.metric_name.as_deref().unwrap_or("");
            format!(
                "{}({metric}) {} {} over {}s",
                rule.agg.unwrap_or(Agg::Avg).label(),
                rule.comparator.symbol(),
                rule.threshold,
                rule.window_secs
            )
        }
        RuleKind::TelemetryCount => format!(
            "count({}) {} {} over {}s",
            source_desc(rule),
            rule.comparator.symbol(),
            rule.threshold,
            rule.window_secs
        ),
        RuleKind::ErrorRatio => format!(
            "error_ratio({}) {} {} over {}s",
            source_desc(rule),
            rule.comparator.symbol(),
            rule.threshold,
            rule.window_secs
        ),
        RuleKind::SpanDuration => {
            let op = rule
                .operation
                .as_deref()
                .map(|o| format!(" op={o}"))
                .unwrap_or_default();
            let err = if rule.error_only { " errors" } else { "" };
            format!(
                "{}(span.duration_ms service={}{op}{err}) {} {}ms over {}s",
                rule.agg.unwrap_or(Agg::P95).label(),
                rule.service.as_deref().unwrap_or("*"),
                rule.comparator.symbol(),
                rule.threshold,
                rule.window_secs
            )
        }
        RuleKind::RelativeChange => format!(
            "change({}) {} {}x vs previous {}s",
            source_desc(rule),
            rule.comparator.symbol(),
            rule.threshold,
            rule.window_secs
        ),
    }
}

/// Description compacte d'une source et de ses filtres (pour `describe`).
fn source_desc(rule: &Rule) -> String {
    let mut parts = vec![match rule.source {
        Source::Logs => "logs",
        Source::Events => "events",
        Source::Spans => "spans",
        Source::Metrics => "metrics",
    }
    .to_string()];
    if let Some(s) = &rule.service {
        parts.push(format!("service={s}"));
    }
    if let Some(sv) = rule.severity_min {
        parts.push(format!("severity>={sv}"));
    }
    if let Some(e) = &rule.event_name {
        parts.push(format!("event={e}"));
    }
    if let Some(o) = &rule.operation {
        parts.push(format!("op={o}"));
    }
    if let Some(m) = &rule.metric_name {
        parts.push(format!("metric={m}"));
    }
    if rule.error_only {
        parts.push("errors".to_string());
    }
    parts.join(" ")
}

/// Calcule la valeur scalaire courante d'une règle sur sa fenêtre `[now - window, now]`.
async fn compute_value(pool: &PgPool, rule: &Rule, now: DateTime<Utc>) -> anyhow::Result<f64> {
    let from = now - chrono::Duration::seconds(rule.window_secs as i64);
    match rule.kind {
        // Routé vers `compute_groups` ; jamais atteint ici.
        RuleKind::LogGroupCount => anyhow::bail!("log_group_count n'est pas une règle scalaire"),
        RuleKind::LogCount | RuleKind::TelemetryCount => compute_count(pool, rule, from, now).await,
        RuleKind::MetricThreshold => compute_metric(pool, rule, from, now).await,
        RuleKind::ErrorRatio => compute_error_ratio(pool, rule, from, now).await,
        RuleKind::SpanDuration => compute_span_duration(pool, rule, from, now).await,
        RuleKind::RelativeChange => compute_relative_change(pool, rule, from, now).await,
    }
}

// ── Helpers SQL partagés (source → table/colonne, fenêtre, filtres) ───────────

/// Table SQL d'une source (identifiant en dur, jamais issu de l'entrée utilisateur).
fn table_of(source: Source) -> &'static str {
    match source {
        Source::Logs => "logs",
        Source::Events => "events",
        Source::Spans => "spans",
        Source::Metrics => "metric_points",
    }
}

/// Colonne temporelle (= clé de partition) d'une source.
fn time_col(source: Source) -> &'static str {
    match source {
        Source::Logs => "log_time",
        Source::Events => "timestamp_client",
        Source::Spans => "start_time",
        Source::Metrics => "time",
    }
}

/// `SELECT <select> FROM <table> WHERE <time_col> BETWEEN from AND to`. `select`, table et colonne
/// proviennent d'énumérations/litéraux internes (jamais de l'entrée) — interpolation sûre ; les
/// bornes temporelles sont bindées.
fn windowed_query<'a>(
    select: &str,
    source: Source,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> sqlx::QueryBuilder<'a, sqlx::Postgres> {
    let tc = time_col(source);
    let mut qb = sqlx::QueryBuilder::new(format!(
        "SELECT {select} FROM {} WHERE {tc} >= ",
        table_of(source)
    ));
    qb.push_bind(from)
        .push(format!(" AND {tc} <= "))
        .push_bind(to);
    qb
}

/// Filtres de base d'une source (hors sévérité/erreur), appliqués au numérateur **et** au
/// dénominateur des ratios.
fn push_base_filter(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, rule: &Rule) {
    match rule.source {
        Source::Logs => {
            if let Some(s) = &rule.service {
                qb.push(" AND service_name = ").push_bind(s.clone());
            }
        }
        Source::Spans => {
            if let Some(s) = &rule.service {
                qb.push(" AND service_name = ").push_bind(s.clone());
            }
            if let Some(o) = &rule.operation {
                qb.push(" AND name = ").push_bind(o.clone());
            }
        }
        Source::Events => {
            if let Some(e) = &rule.event_name {
                qb.push(" AND event_name = ").push_bind(e.clone());
            }
        }
        Source::Metrics => {
            if let Some(s) = &rule.service {
                qb.push(" AND service_name = ").push_bind(s.clone());
            }
            if let Some(m) = &rule.metric_name {
                qb.push(" AND metric_name = ").push_bind(m.clone());
            }
        }
    }
}

/// Filtres d'un *compte* : base + sévérité minimale (logs) + restriction aux erreurs (`error_only`).
fn push_count_filter(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, rule: &Rule) {
    push_base_filter(qb, rule);
    if rule.source == Source::Logs {
        if let Some(sv) = rule.severity_min {
            qb.push(" AND severity_number >= ").push_bind(sv);
        }
    }
    if rule.error_only {
        push_error_predicate(qb, rule);
    }
}

/// Prédicat « ligne en erreur » (numérateur d'`error_ratio`) : sévérité (logs) ou status=error (spans).
fn push_error_predicate(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, rule: &Rule) {
    match rule.source {
        Source::Logs => {
            qb.push(" AND severity_number >= ")
                .push_bind(rule.severity_min.unwrap_or(17));
        }
        Source::Spans => {
            qb.push(" AND status_code = 2");
        }
        Source::Events | Source::Metrics => {}
    }
}

/// Expression SQL d'un agrégat sur `val` (colonne/expression numérique). `Last` est géré à part.
fn agg_sql_expr(agg: Agg, val: &str) -> String {
    match agg.percentile() {
        Some(p) => format!("percentile_cont({p}) WITHIN GROUP (ORDER BY {val})"),
        None => match agg {
            Agg::Avg => format!("avg({val})"),
            Agg::Max => format!("max({val})"),
            Agg::Min => format!("min({val})"),
            Agg::Sum => format!("sum({val})"),
            Agg::Count => format!("count({val})"),
            Agg::Last => unreachable!("last géré séparément"),
            _ => unreachable!("percentiles gérés ci-dessus"),
        },
    }
}

// ── Calculs par kind ──────────────────────────────────────────────────────────

/// `log_count` / `telemetry_count` : compte de lignes filtrées sur la source.
async fn compute_count(
    pool: &PgPool,
    rule: &Rule,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<f64> {
    let mut qb = windowed_query("count(*)", rule.source, from, to);
    push_count_filter(&mut qb, rule);
    let count: i64 = qb.build_query_scalar().fetch_one(pool).await?;
    Ok(count as f64)
}

/// `metric_threshold` : agrégat (avg/max/min/sum/count/last/p50…p99) d'une métrique.
async fn compute_metric(
    pool: &PgPool,
    rule: &Rule,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<f64> {
    let agg = rule.agg.unwrap_or(Agg::Avg);
    // Valeur scalaire d'un point : value_double sinon value_int.
    let val = "coalesce(value_double, value_int::double precision)";
    let metric = rule.metric_name.clone().unwrap_or_default();

    let value: Option<f64> = if agg == Agg::Last {
        // `last` : point le plus récent (requête dédiée).
        let mut qb = windowed_query(val, Source::Metrics, from, to);
        qb.push(" AND metric_name = ").push_bind(metric);
        if let Some(s) = &rule.service {
            qb.push(" AND service_name = ").push_bind(s.clone());
        }
        qb.push(" ORDER BY time DESC LIMIT 1");
        qb.build_query_scalar().fetch_optional(pool).await?
    } else {
        let mut qb = windowed_query(&agg_sql_expr(agg, val), Source::Metrics, from, to);
        qb.push(" AND metric_name = ").push_bind(metric);
        if let Some(s) = &rule.service {
            qb.push(" AND service_name = ").push_bind(s.clone());
        }
        qb.build_query_scalar().fetch_optional(pool).await?
    };
    // Aucune donnée sur la fenêtre → 0.0 (pas de déclenchement pour gt/gte usuels).
    Ok(value.unwrap_or(0.0))
}

/// `error_ratio` : fraction de lignes en erreur (numérateur) sur le total (dénominateur), avec
/// garde-fou `min_count` (sous l'échantillon minimal, 0.0 — pas de déclenchement).
async fn compute_error_ratio(
    pool: &PgPool,
    rule: &Rule,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<f64> {
    let mut q_total = windowed_query("count(*)", rule.source, from, to);
    push_base_filter(&mut q_total, rule);
    let total: i64 = q_total.build_query_scalar().fetch_one(pool).await?;
    if (total as u64) < rule.min_count.max(1) {
        return Ok(0.0);
    }
    let mut q_err = windowed_query("count(*)", rule.source, from, to);
    push_base_filter(&mut q_err, rule);
    push_error_predicate(&mut q_err, rule);
    let errors: i64 = q_err.build_query_scalar().fetch_one(pool).await?;
    Ok(errors as f64 / total as f64)
}

/// `span_duration` : agrégat de la latence des spans (`duration_ms`, en ms).
async fn compute_span_duration(
    pool: &PgPool,
    rule: &Rule,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<f64> {
    let agg = rule.agg.unwrap_or(Agg::P95);
    let value: Option<f64> = if agg == Agg::Last {
        let mut qb = windowed_query("duration_ms", Source::Spans, from, to);
        qb.push(" AND duration_ms IS NOT NULL");
        push_span_filter(&mut qb, rule);
        qb.push(" ORDER BY start_time DESC LIMIT 1");
        qb.build_query_scalar().fetch_optional(pool).await?
    } else {
        let mut qb = windowed_query(&agg_sql_expr(agg, "duration_ms"), Source::Spans, from, to);
        qb.push(" AND duration_ms IS NOT NULL");
        push_span_filter(&mut qb, rule);
        qb.build_query_scalar().fetch_optional(pool).await?
    };
    Ok(value.unwrap_or(0.0))
}

/// Filtres propres aux spans (`span_duration`) : service / opération / erreurs.
fn push_span_filter(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, rule: &Rule) {
    if let Some(s) = &rule.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(o) = &rule.operation {
        qb.push(" AND name = ").push_bind(o.clone());
    }
    if rule.error_only {
        qb.push(" AND status_code = 2");
    }
}

/// `relative_change` : ratio volume(fenêtre courante) / volume(fenêtre précédente, même durée).
/// La base est plafonnée par `max(min_count, 1)` pour éviter les faux pics sans historique.
async fn compute_relative_change(
    pool: &PgPool,
    rule: &Rule,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<f64> {
    let window = to - from;
    let prev_from = from - window;

    let mut q_cur = windowed_query("count(*)", rule.source, from, to);
    push_count_filter(&mut q_cur, rule);
    let current: i64 = q_cur.build_query_scalar().fetch_one(pool).await?;

    let mut q_prev = windowed_query("count(*)", rule.source, prev_from, from);
    push_count_filter(&mut q_prev, rule);
    let previous: i64 = q_prev.build_query_scalar().fetch_one(pool).await?;

    let base = (previous as f64).max(rule.min_count.max(1) as f64);
    Ok(current as f64 / base)
}

/// Calcule le compte de logs **par groupe** (`group_by`) sur la fenêtre `[now - window, now]`.
/// Retourne `(clé de groupe, compte)`. Les groupes à clé NULL sont ignorés.
async fn compute_groups(
    pool: &PgPool,
    rule: &Rule,
    now: DateTime<Utc>,
) -> anyhow::Result<Vec<(String, f64)>> {
    let from = now - chrono::Duration::seconds(rule.window_secs as i64);
    let group = rule.group_expr()?;

    // SELECT <expr> AS gk, count(*) … GROUP BY <expr>. L'expression de groupe (colonne en liste
    // blanche, ou `log_attributes ->> $clé` bindée) apparaît dans le SELECT et le GROUP BY.
    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
    push_group_expr(&mut qb, &group);
    qb.push(" AS gk, count(*) FROM logs WHERE log_time >= ")
        .push_bind(from)
        .push(" AND log_time <= ")
        .push_bind(now);
    if let Some(s) = &rule.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(sv) = rule.severity_min {
        qb.push(" AND severity_number >= ").push_bind(sv);
    }
    qb.push(" GROUP BY ");
    push_group_expr(&mut qb, &group);

    let rows: Vec<(Option<String>, i64)> = qb.build_query_as().fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .filter_map(|(gk, count)| gk.map(|gk| (gk, count as f64)))
        .collect())
}

/// Pousse l'expression de regroupement dans le `QueryBuilder` (colonne sûre ou attribut bindé).
fn push_group_expr(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, group: &GroupExpr) {
    match group {
        // Identifiant en liste blanche (cf. `Rule::group_expr`) : sûr à interpoler.
        GroupExpr::Column(col) => {
            qb.push(*col);
        }
        // Clé d'attribut : bindée comme paramètre (anti-injection).
        GroupExpr::Attr(key) => {
            qb.push("log_attributes ->> ").push_bind(key.clone());
        }
    }
}

/// Boucle de fond : évalue toutes les règles à intervalle régulier jusqu'au shutdown.
pub async fn run_eval_loop(
    pool: PgPool,
    rules: Vec<Rule>,
    dispatcher: Dispatcher,
    eval_interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut state = AlertEngineState::new();
    let mut ticker = interval(eval_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tracing::info!(rules = rules.len(), "moteur d'alerting démarré");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("moteur d'alerting arrêté");
                break;
            }
            _ = ticker.tick() => {
                let n = evaluate_once(&pool, &rules, &mut state, &dispatcher, Utc::now()).await;
                if n > 0 {
                    tracing::info!(notified = n, "alertes notifiées");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::rules::{Comparator, RuleKind};

    fn metric_rule() -> Rule {
        Rule {
            name: "latence".into(),
            kind: RuleKind::MetricThreshold,
            service: Some("api".into()),
            metric_name: Some("http.server.duration".into()),
            agg: Some(Agg::Avg),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 500.0,
            cooldown_secs: 600,
            severity: "critical".into(),
            ..Default::default()
        }
    }

    #[test]
    fn describe_metric_and_log_rules() {
        let m = describe(&metric_rule());
        assert!(
            m.contains("avg(http.server.duration) > 500 over 300s"),
            "{m}"
        );

        let log = Rule {
            name: "errs".into(),
            kind: RuleKind::LogCount,
            service: Some("billing".into()),
            severity_min: Some(17),
            window_secs: 60,
            comparator: Comparator::Gte,
            threshold: 5.0,
            ..Default::default()
        };
        let d = describe(&log);
        assert!(
            d.contains("count(logs service=billing, severity>=17) >= 5 over 60s"),
            "{d}"
        );

        let grp = Rule {
            name: "erreurs identiques".into(),
            kind: RuleKind::LogGroupCount,
            severity_min: Some(17),
            window_secs: 300,
            comparator: Comparator::Gte,
            threshold: 5.0,
            group_by: Some("body".into()),
            ..Default::default()
        };
        let g = describe(&grp);
        assert!(
            g.contains("count(logs service=*, severity>=17) by body >= 5 over 300s"),
            "{g}"
        );
    }

    #[test]
    fn describe_standard_kinds() {
        // metric percentile
        let p95 = Rule {
            name: "p95".into(),
            kind: RuleKind::MetricThreshold,
            metric_name: Some("http.server.duration".into()),
            agg: Some(Agg::P95),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 800.0,
            ..Default::default()
        };
        assert!(describe(&p95).contains("p95(http.server.duration) > 800 over 300s"));

        // heartbeat (telemetry_count, metrics, lte 0)
        let hb = Rule {
            name: "heartbeat".into(),
            kind: RuleKind::TelemetryCount,
            source: Source::Metrics,
            service: Some("api".into()),
            window_secs: 300,
            comparator: Comparator::Lte,
            threshold: 0.0,
            ..Default::default()
        };
        assert!(describe(&hb).contains("count(metrics service=api) <= 0 over 300s"));

        // error_ratio spans
        let er = Rule {
            name: "err".into(),
            kind: RuleKind::ErrorRatio,
            source: Source::Spans,
            service: Some("api".into()),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 0.05,
            ..Default::default()
        };
        assert!(describe(&er).contains("error_ratio(spans service=api) > 0.05 over 300s"));

        // span_duration p99 with operation
        let sd = Rule {
            name: "checkout".into(),
            kind: RuleKind::SpanDuration,
            agg: Some(Agg::P99),
            service: Some("api".into()),
            operation: Some("checkout".into()),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 2000.0,
            ..Default::default()
        };
        assert!(describe(&sd)
            .contains("p99(span.duration_ms service=api op=checkout) > 2000ms over 300s"));

        // relative_change
        let rc = Rule {
            name: "spike".into(),
            kind: RuleKind::RelativeChange,
            source: Source::Logs,
            severity_min: Some(17),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 3.0,
            ..Default::default()
        };
        assert!(describe(&rc).contains("change(logs severity>=17) > 3x vs previous 300s"));
    }

    #[test]
    fn agg_expr_shapes() {
        assert_eq!(agg_sql_expr(Agg::Avg, "v"), "avg(v)");
        assert_eq!(agg_sql_expr(Agg::Count, "v"), "count(v)");
        assert_eq!(
            agg_sql_expr(Agg::P95, "v"),
            "percentile_cont(0.95) WITHIN GROUP (ORDER BY v)"
        );
    }
}
