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
    /// Compte de logs **groupés par signature** (`group_by`) : déclenche par groupe dont le
    /// compte franchit le seuil. Ex. « 5 erreurs identiques » → un webhook par message distinct.
    LogGroupCount,
    /// Agrégat d'une métrique sur la fenêtre.
    MetricThreshold,
}

/// Action déclenchée quand une règle passe en `firing`/`resolved`. Modulable et configurable
/// par règle (`actions`). Si la liste est vide, les notifiers globaux par défaut sont utilisés.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    /// Message Slack. `webhook_url` optionnel : sinon `SLACK_WEBHOOK_URL` global.
    Slack {
        #[serde(default)]
        webhook_url: Option<String>,
    },
    /// E-mail. `to` optionnel : sinon `ALERT_EMAIL_TO` global.
    Email {
        #[serde(default)]
        to: Option<Vec<String>>,
    },
    /// Webhook HTTP générique : POST JSON de l'alerte sur `url`, avec en-têtes optionnels.
    Webhook {
        url: String,
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
    },
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

    /// Pour `log_group_count` : clé de regroupement. Colonne (`body`, `service_name`,
    /// `severity_text`, `trace_id`) ou `attr:<clé>` (attribut de log). Défaut `body`.
    #[serde(default)]
    pub group_by: Option<String>,

    /// Actions déclenchées (slack/email/webhook). Vide ⇒ notifiers globaux par défaut.
    #[serde(default)]
    pub actions: Vec<Action>,
}

/// Expression de regroupement validée pour `log_group_count` (anti-injection).
#[derive(Debug, Clone)]
pub enum GroupExpr {
    /// Colonne en liste blanche.
    Column(&'static str),
    /// Clé d'attribut JSON (bindée comme paramètre).
    Attr(String),
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
            RuleKind::LogGroupCount => {
                self.group_expr()?;
            }
            RuleKind::LogCount => {}
        }
        for action in &self.actions {
            if let Action::Webhook { url, .. } = action {
                if url.trim().is_empty() {
                    anyhow::bail!("règle '{}': action webhook sans url", self.name);
                }
            }
        }
        Ok(())
    }

    /// Expression de regroupement validée (défaut `body`). Erreur si `group_by` non supporté.
    pub fn group_expr(&self) -> Result<GroupExpr> {
        match self.group_by.as_deref().unwrap_or("body") {
            "body" => Ok(GroupExpr::Column("body")),
            "service_name" => Ok(GroupExpr::Column("service_name")),
            "severity_text" => Ok(GroupExpr::Column("severity_text")),
            "trace_id" => Ok(GroupExpr::Column("trace_id")),
            other => match other.strip_prefix("attr:") {
                Some(key) if !key.is_empty() => Ok(GroupExpr::Attr(key.to_string())),
                _ => anyhow::bail!(
                    "règle '{}': group_by non supporté '{other}' \
                     (body|service_name|severity_text|trace_id|attr:<clé>)",
                    self.name
                ),
            },
        }
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

    #[test]
    fn parses_group_count_and_actions() {
        let raw = r#"{ "rules": [
            { "name":"5 erreurs identiques", "kind":"log_group_count", "severity_min":17,
              "group_by":"body", "window_secs":300, "comparator":"gte", "threshold":5,
              "actions":[
                { "type":"webhook", "url":"https://h/x", "headers": { "x-a": "1" } },
                { "type":"slack" },
                { "type":"email", "to": ["a@b.c"] }
              ] }
        ] }"#;
        let rules = parse_rules(raw).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].kind, RuleKind::LogGroupCount);
        assert_eq!(rules[0].actions.len(), 3);
        assert!(matches!(
            rules[0].group_expr().unwrap(),
            GroupExpr::Column("body")
        ));
    }

    #[test]
    fn group_by_attr_and_rejects_unknown() {
        let attr = r#"{ "rules": [
            { "name":"par code", "kind":"log_group_count", "group_by":"attr:error.code",
              "window_secs":60, "comparator":"gte", "threshold":3 }
        ] }"#;
        let rules = parse_rules(attr).unwrap();
        assert!(matches!(&rules[0].group_expr().unwrap(), GroupExpr::Attr(k) if k == "error.code"));

        let bad = r#"{ "rules": [
            { "name":"x", "kind":"log_group_count", "group_by":"DROP TABLE",
              "window_secs":60, "comparator":"gte", "threshold":3 }
        ] }"#;
        assert!(
            parse_rules(bad).is_err(),
            "group_by hors liste blanche rejeté"
        );
    }

    #[test]
    fn rejects_webhook_action_without_url() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"log_count", "window_secs":60, "comparator":"gt", "threshold":1,
              "actions":[ { "type":"webhook", "url":"" } ] }
        ] }"#;
        assert!(parse_rules(raw).is_err());
    }

    #[test]
    fn example_rules_file_parses() {
        // Garde l'exemple livré (`backend/alert_rules.example.json`) cohérent avec le schéma.
        let rules = load_rules(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/alert_rules.example.json"
        ))
        .expect("alert_rules.example.json doit parser et valider");
        assert!(rules.len() >= 4);
    }
}
