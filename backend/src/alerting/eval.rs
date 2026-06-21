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
use crate::alerting::rules::{Agg, GroupExpr, Rule, RuleKind};

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
            let agg = match rule.agg.unwrap_or(Agg::Avg) {
                Agg::Avg => "avg",
                Agg::Max => "max",
                Agg::Last => "last",
            };
            let metric = rule.metric_name.as_deref().unwrap_or("");
            format!(
                "{agg}({metric}) {} {} over {}s",
                rule.comparator.symbol(),
                rule.threshold,
                rule.window_secs
            )
        }
    }
}

/// Calcule la valeur courante d'une règle sur sa fenêtre glissante `[now - window, now]`.
async fn compute_value(pool: &PgPool, rule: &Rule, now: DateTime<Utc>) -> anyhow::Result<f64> {
    let from = now - chrono::Duration::seconds(rule.window_secs as i64);
    match rule.kind {
        // Routé vers `compute_groups` ; jamais atteint ici.
        RuleKind::LogGroupCount => anyhow::bail!("log_group_count n'est pas une règle scalaire"),
        RuleKind::LogCount => {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "SELECT count(*) FROM logs WHERE log_time >= ",
            );
            qb.push_bind(from).push(" AND log_time <= ").push_bind(now);
            if let Some(s) = &rule.service {
                qb.push(" AND service_name = ").push_bind(s.clone());
            }
            if let Some(sv) = rule.severity_min {
                qb.push(" AND severity_number >= ").push_bind(sv);
            }
            let count: i64 = qb.build_query_scalar().fetch_one(pool).await?;
            Ok(count as f64)
        }
        RuleKind::MetricThreshold => {
            // Valeur scalaire d'un point : value_double sinon value_int.
            let expr = match rule.agg.unwrap_or(Agg::Avg) {
                Agg::Avg => "avg(coalesce(value_double, value_int::double precision))",
                Agg::Max => "max(coalesce(value_double, value_int::double precision))",
                // `last` : valeur du point le plus récent (ordre par time décroissant).
                Agg::Last => {
                    "(coalesce(value_double, value_int::double precision)) \
                     FILTER (WHERE true) ORDER BY time DESC LIMIT 1"
                }
            };
            let metric_name = rule.metric_name.clone().unwrap_or_default();

            let value: Option<f64> = if rule.agg == Some(Agg::Last) {
                // `last` se prête mal à un agrégat : requête dédiée (point le plus récent).
                let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                    "SELECT coalesce(value_double, value_int::double precision) \
                     FROM metric_points WHERE metric_name = ",
                );
                qb.push_bind(metric_name)
                    .push(" AND time >= ")
                    .push_bind(from)
                    .push(" AND time <= ")
                    .push_bind(now);
                if let Some(s) = &rule.service {
                    qb.push(" AND service_name = ").push_bind(s.clone());
                }
                qb.push(" ORDER BY time DESC LIMIT 1");
                qb.build_query_scalar().fetch_optional(pool).await?
            } else {
                let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
                    "SELECT {expr} FROM metric_points WHERE metric_name = "
                ));
                qb.push_bind(metric_name)
                    .push(" AND time >= ")
                    .push_bind(from)
                    .push(" AND time <= ")
                    .push_bind(now);
                if let Some(s) = &rule.service {
                    qb.push(" AND service_name = ").push_bind(s.clone());
                }
                qb.build_query_scalar().fetch_one(pool).await?
            };
            // Aucune donnée sur la fenêtre → 0.0 (pas de déclenchement pour gt/gte usuels).
            Ok(value.unwrap_or(0.0))
        }
    }
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
            severity_min: None,
            metric_name: Some("http.server.duration".into()),
            agg: Some(Agg::Avg),
            window_secs: 300,
            comparator: Comparator::Gt,
            threshold: 500.0,
            cooldown_secs: 600,
            severity: "critical".into(),
            group_by: None,
            actions: vec![],
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
            metric_name: None,
            agg: None,
            window_secs: 60,
            comparator: Comparator::Gte,
            threshold: 5.0,
            cooldown_secs: 0,
            severity: "warning".into(),
            group_by: None,
            actions: vec![],
        };
        let d = describe(&log);
        assert!(
            d.contains("count(logs service=billing, severity>=17) >= 5 over 60s"),
            "{d}"
        );

        let grp = Rule {
            name: "erreurs identiques".into(),
            kind: RuleKind::LogGroupCount,
            service: None,
            severity_min: Some(17),
            metric_name: None,
            agg: None,
            window_secs: 300,
            comparator: Comparator::Gte,
            threshold: 5.0,
            cooldown_secs: 0,
            severity: "warning".into(),
            group_by: Some("body".into()),
            actions: vec![],
        };
        let g = describe(&grp);
        assert!(
            g.contains("count(logs service=*, severity>=17) by body >= 5 over 300s"),
            "{g}"
        );
    }
}
