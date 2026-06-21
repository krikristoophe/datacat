//! Évaluateur d'alertes : tâche de fond périodique + fonction `evaluate_once` testable.
//!
//! Pour chaque règle, une requête SQL calcule la valeur courante sur la fenêtre, comparée au
//! seuil. Une machine à états par règle (ok ↔ firing) avec **cooldown** garantit qu'on ne
//! notifie qu'aux transitions (ok→firing, et firing→ok = résolu), et au plus une fois par
//! `cooldown_secs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};

use crate::alerting::notify::{Alert, AlertState, Notifier};
use crate::alerting::rules::{Agg, Rule, RuleKind};

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
/// Retourne le nombre d'alertes effectivement notifiées.
pub async fn evaluate_once(
    pool: &PgPool,
    rules: &[Rule],
    state: &mut AlertEngineState,
    notifiers: &[Arc<dyn Notifier>],
    now: DateTime<Utc>,
) -> usize {
    let mut notified = 0;
    for rule in rules {
        let value = match compute_value(pool, rule, now).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(rule = %rule.name, error = %e, "évaluation de la règle échouée");
                continue;
            }
        };
        let breaching = rule.comparator.compare(value, rule.threshold);
        let entry = state.entry(rule.name.clone()).or_default();

        // Transition à notifier ?
        let transition = match (entry.firing, breaching) {
            (false, true) => Some(AlertState::Firing),
            (true, false) => Some(AlertState::Resolved),
            _ => None,
        };
        entry.firing = breaching;

        let Some(alert_state) = transition else {
            continue;
        };

        // Cooldown : pas plus d'une notification par `cooldown_secs` pour cette règle.
        if let Some(last) = entry.last_notified {
            let elapsed = (now - last).to_std().unwrap_or(Duration::ZERO);
            if elapsed < Duration::from_secs(rule.cooldown_secs) {
                continue;
            }
        }

        let alert = Alert {
            rule_name: rule.name.clone(),
            severity: rule.severity.clone(),
            state: alert_state,
            value,
            threshold: rule.threshold,
            description: describe(rule),
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
            notified += 1;
        }
    }
    notified
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

/// Boucle de fond : évalue toutes les règles à intervalle régulier jusqu'au shutdown.
pub async fn run_eval_loop(
    pool: PgPool,
    rules: Vec<Rule>,
    notifiers: Vec<Arc<dyn Notifier>>,
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
                let n = evaluate_once(&pool, &rules, &mut state, &notifiers, Utc::now()).await;
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
        };
        let d = describe(&log);
        assert!(
            d.contains("count(logs service=billing, severity>=17) >= 5 over 60s"),
            "{d}"
        );
    }
}
