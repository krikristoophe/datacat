//! Schéma des règles d'alerting et chargement depuis un fichier JSON (`ALERT_RULES_FILE`).
//!
//! Deux familles de règles :
//! - `log_count` : compte de logs filtrés (service, sévérité minimale) sur une fenêtre glissante.
//! - `metric_threshold` : agrégat (avg / max / last) d'une métrique sur une fenêtre glissante.
//!
//! Chaque règle compare la valeur calculée à un `threshold` via un `comparator` (gt/gte/lt/lte),
//! avec un `cooldown_secs` qui borne la fréquence des notifications.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Type de règle (détermine la requête d'évaluation).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuleKind {
    /// Compte de logs sur la fenêtre (filtré par service / sévérité minimale).
    LogCount,
    /// Agrégat d'une métrique sur la fenêtre.
    MetricThreshold,
}

/// Fonction d'agrégation pour `metric_threshold`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Agg {
    Avg,
    Max,
    Last,
}

/// Comparateur valeur ↔ seuil.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Comparator {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Comparator {
    /// Vrai si `value <comparator> threshold` (condition de déclenchement).
    pub fn compare(&self, value: f64, threshold: f64) -> bool {
        match self {
            Comparator::Gt => value > threshold,
            Comparator::Gte => value >= threshold,
            Comparator::Lt => value < threshold,
            Comparator::Lte => value <= threshold,
        }
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            Comparator::Gt => ">",
            Comparator::Gte => ">=",
            Comparator::Lt => "<",
            Comparator::Lte => "<=",
        }
    }
}

fn default_severity() -> String {
    "warning".to_string()
}

/// Une règle d'alerting. Les champs `metric_name` / `agg` ne sont requis que pour
/// `metric_threshold` ; `severity_min` ne s'applique qu'à `log_count`.
#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    /// Nom lisible (apparaît dans la notification et identifie l'état de la règle).
    pub name: String,
    pub kind: RuleKind,
    /// Filtre `service.name` (optionnel : toutes sources si absent).
    #[serde(default)]
    pub service: Option<String>,

    // ── log_count ──
    /// Sévérité OTLP minimale prise en compte (ex. 17 = ERROR).
    #[serde(default)]
    pub severity_min: Option<i16>,

    // ── metric_threshold ──
    #[serde(default)]
    pub metric_name: Option<String>,
    #[serde(default)]
    pub agg: Option<Agg>,

    /// Fenêtre glissante (secondes) sur laquelle la valeur est calculée.
    pub window_secs: u64,
    pub comparator: Comparator,
    pub threshold: f64,
    /// Durée minimale entre deux notifications pour cette règle.
    #[serde(default)]
    pub cooldown_secs: u64,
    /// Sévérité de l'alerte émise (libre : info/warning/critical…).
    #[serde(default = "default_severity")]
    pub severity: String,
}

impl Rule {
    /// Valide la cohérence de la règle selon son `kind`.
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            anyhow::bail!("règle sans nom");
        }
        if self.window_secs == 0 {
            anyhow::bail!("règle '{}': window_secs doit être > 0", self.name);
        }
        match self.kind {
            RuleKind::MetricThreshold => {
                if self.metric_name.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("règle '{}': metric_threshold exige metric_name", self.name);
                }
                if self.agg.is_none() {
                    anyhow::bail!(
                        "règle '{}': metric_threshold exige agg (avg|max|last)",
                        self.name
                    );
                }
            }
            RuleKind::LogCount => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RulesFile {
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// Charge et valide les règles depuis un fichier JSON.
pub fn load_rules(path: impl AsRef<Path>) -> Result<Vec<Rule>> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("lecture du fichier de règles {}", path.display()))?;
    parse_rules(&raw).with_context(|| format!("parsing de {}", path.display()))
}

/// Parse et valide les règles depuis une chaîne JSON (testable sans fichier).
pub fn parse_rules(raw: &str) -> Result<Vec<Rule>> {
    let file: RulesFile = serde_json::from_str(raw).context("JSON de règles invalide")?;
    for r in &file.rules {
        r.validate()?;
    }
    Ok(file.rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_kinds() {
        let raw = r#"{ "rules": [
            { "name":"erreurs billing", "kind":"log_count", "service":"billing",
              "severity_min":17, "window_secs":300, "comparator":"gt", "threshold":10,
              "cooldown_secs":600, "severity":"critical" },
            { "name":"latence", "kind":"metric_threshold", "metric_name":"http.server.duration",
              "service":"api", "agg":"avg", "window_secs":300, "comparator":"gt",
              "threshold":500, "cooldown_secs":600 }
        ] }"#;
        let rules = parse_rules(raw).unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].kind, RuleKind::LogCount);
        assert_eq!(rules[0].severity_min, Some(17));
        assert_eq!(rules[0].severity, "critical");
        assert_eq!(rules[1].kind, RuleKind::MetricThreshold);
        assert_eq!(rules[1].agg, Some(Agg::Avg));
        // défaut de sévérité quand absent.
        assert_eq!(rules[1].severity, "warning");
    }

    #[test]
    fn rejects_metric_rule_without_metric_name() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"metric_threshold", "agg":"avg",
              "window_secs":60, "comparator":"gt", "threshold":1 }
        ] }"#;
        assert!(parse_rules(raw).is_err());
    }

    #[test]
    fn comparator_logic() {
        assert!(Comparator::Gt.compare(2.0, 1.0));
        assert!(!Comparator::Gt.compare(1.0, 1.0));
        assert!(Comparator::Gte.compare(1.0, 1.0));
        assert!(Comparator::Lt.compare(0.0, 1.0));
        assert!(Comparator::Lte.compare(1.0, 1.0));
    }
}
