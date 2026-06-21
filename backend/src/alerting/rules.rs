//! Schéma des règles d'alerting et chargement depuis un fichier JSON (`ALERT_RULES_FILE`).
//!
//! Familles de règles (cas d'usage standard d'observabilité) :
//! - `log_count` : compte de logs filtrés (service, sévérité minimale) sur une fenêtre.
//! - `log_group_count` : compte de logs groupés par signature (`group_by`) — un seuil par groupe.
//! - `metric_threshold` : agrégat (avg / max / min / sum / count / last / p50…p99) d'une métrique.
//! - `telemetry_count` : compte de lignes sur une `source` (logs/events/spans/metrics) — couvre le
//!   *heartbeat / no-data* (comparateur `lte` 0), la chute de trafic (`lt`) et le pic (`gt`).
//! - `error_ratio` : taux d'erreur sur `logs` (sévérité) ou `spans` (status=error).
//! - `span_duration` : agrégat de la latence des spans (`duration_ms`) — p95/p99 SLO.
//! - `relative_change` : variation relative du volume vs la fenêtre précédente (détection de pic).
//! - `composite` : combine plusieurs sous-conditions par `op` (`all` = ET, `any` = OU).
//! - `log_new_signature` : signature de log **jamais vue** sur une fenêtre `baseline` (first-seen).
//! - `anomaly` : z-score du volume courant vs une baseline glissante (anomalie statistique).
//!
//! Chaque règle compare la valeur calculée à un `threshold` via un `comparator` (gt/gte/lt/lte),
//! avec un `cooldown_secs` qui borne la fréquence des notifications.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Type de règle (détermine la requête d'évaluation).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuleKind {
    /// Compte de logs sur la fenêtre (filtré par service / sévérité minimale).
    #[default]
    LogCount,
    /// Compte de logs **groupés par signature** (`group_by`) : déclenche par groupe dont le
    /// compte franchit le seuil. Ex. « 5 erreurs identiques » → un webhook par message distinct.
    LogGroupCount,
    /// Agrégat d'une métrique sur la fenêtre (avg/max/min/sum/count/last/p50…p99).
    MetricThreshold,
    /// Compte de lignes sur une `source` (logs/events/spans/metrics). Heartbeat/no-data via
    /// `lte` 0, chute de trafic via `lt`, pic de volume via `gt`.
    TelemetryCount,
    /// Taux d'erreur (fraction 0..1) sur `logs` (sévérité ≥ `severity_min`) ou `spans`
    /// (status=error). Garde-fou `min_count` pour ignorer les petits échantillons.
    ErrorRatio,
    /// Agrégat de la latence des spans (`duration_ms`, en ms) — `agg` avg/max/min/p50…p99
    /// (défaut p95). Filtrable par `service` / `operation` (nom du span) / `error_only`.
    SpanDuration,
    /// Ratio volume(fenêtre courante) / volume(fenêtre précédente) sur une `source`. `gt 2` =
    /// « doublé », `lt 0.5` = « divisé par deux ». Garde-fou `min_count` sur la base.
    RelativeChange,
    /// Combine plusieurs sous-conditions (`conditions`) par `op` (`all` = ET, `any` = OU). Chaque
    /// sous-condition est une règle scalaire (avec son propre kind/fenêtre/seuil).
    Composite,
    /// Signature de log (`group_by`) présente sur la fenêtre courante mais **absente** de la
    /// fenêtre `baseline_secs` qui précède → première apparition (nouvelle erreur).
    LogNewSignature,
    /// Anomalie statistique : z-score = (volume courant − moyenne) / écart-type, calculé sur des
    /// buckets de `window_secs` couvrant `baseline_secs`. `gt 3` = pic à +3σ.
    Anomaly,
}

/// Opérateur de combinaison des sous-conditions d'un `composite`.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoolOp {
    /// ET logique : toutes les sous-conditions doivent être franchies.
    #[default]
    All,
    /// OU logique : au moins une sous-condition franchie.
    Any,
}

/// Source de données interrogée par les règles génériques (`telemetry_count`, `error_ratio`,
/// `relative_change`). Chaque source a sa table, sa colonne temporelle et ses filtres.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Logs techniques (`logs`, `log_time`). Filtres : `service`, `severity_min`.
    #[default]
    Logs,
    /// Events produit (`events`, `timestamp_client`). Filtre : `event_name`.
    Events,
    /// Spans/traces (`spans`, `start_time`). Filtres : `service`, `operation`, `error_only`.
    Spans,
    /// Points de métriques (`metric_points`, `time`). Filtres : `service`, `metric_name`.
    Metrics,
}

/// Action déclenchée quand une règle passe en `firing`/`resolved`. Modulable et configurable
/// par règle (`actions`). Si la liste est vide, les notifiers globaux par défaut sont utilisés.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    /// Message Slack via le bot configuré. `channel` optionnel : sinon le canal par défaut du bot.
    Slack {
        #[serde(default)]
        channel: Option<String>,
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

/// Fonction d'agrégation pour `metric_threshold` et `span_duration`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Agg {
    Avg,
    Max,
    Min,
    Sum,
    Count,
    /// Valeur du point/span le plus récent.
    Last,
    P50,
    P90,
    P95,
    P99,
}

impl Agg {
    /// Quantile associé (pour `percentile_cont`), `None` pour les agrégats classiques.
    pub fn percentile(&self) -> Option<f64> {
        match self {
            Agg::P50 => Some(0.5),
            Agg::P90 => Some(0.9),
            Agg::P95 => Some(0.95),
            Agg::P99 => Some(0.99),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Agg::Avg => "avg",
            Agg::Max => "max",
            Agg::Min => "min",
            Agg::Sum => "sum",
            Agg::Count => "count",
            Agg::Last => "last",
            Agg::P50 => "p50",
            Agg::P90 => "p90",
            Agg::P95 => "p95",
            Agg::P99 => "p99",
        }
    }
}

/// Comparateur valeur ↔ seuil.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Comparator {
    #[default]
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

/// Une règle d'alerting. Beaucoup de champs sont conditionnels au `kind` (cf. `validate`).
/// `Default` n'est fourni que pour la construction en test ; la désérialisation exige toujours
/// `name`, `kind`, `window_secs`, `comparator`, `threshold`. `deny_unknown_fields` rejette les
/// fautes de frappe (ex. `treshold`) au lieu de les ignorer silencieusement (config écrite à la main).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Nom lisible (apparaît dans la notification et identifie l'état de la règle). Requis pour
    /// une règle de premier niveau ; omis pour une sous-condition de `composite`.
    #[serde(default)]
    pub name: String,
    pub kind: RuleKind,
    /// Source interrogée par `telemetry_count` / `error_ratio` / `relative_change`. Défaut `logs`.
    #[serde(default)]
    pub source: Source,
    /// Filtre `service.name` (optionnel : toutes sources si absent). S'applique à logs/spans/metrics.
    #[serde(default)]
    pub service: Option<String>,
    /// Filtre `tenant_id` (optionnel). Renseigné par défaut par le projet propriétaire de la règle.
    /// S'applique aux sources qui portent un tenant (logs/spans/events ; ignoré pour metrics).
    #[serde(default)]
    pub tenant: Option<String>,

    // ── logs ──
    /// Sévérité OTLP minimale prise en compte (ex. 17 = ERROR).
    #[serde(default)]
    pub severity_min: Option<i16>,

    // ── metric_threshold ──
    #[serde(default)]
    pub metric_name: Option<String>,
    #[serde(default)]
    pub agg: Option<Agg>,

    // ── events ──
    /// Filtre `event_name` (source `events`).
    #[serde(default)]
    pub event_name: Option<String>,

    // ── spans ──
    /// Filtre le nom d'opération du span (source `spans` / `span_duration`).
    #[serde(default)]
    pub operation: Option<String>,
    /// Restreint aux erreurs (spans : status=error ; logs : sévérité ≥ `severity_min`/17).
    #[serde(default)]
    pub error_only: bool,

    /// Échantillon minimal pour `error_ratio` / `relative_change` / `anomaly` : sous ce total
    /// (resp. cette moyenne de base), pas de déclenchement. Évite le bruit sur de petits volumes.
    #[serde(default)]
    pub min_count: u64,

    /// Fenêtre de référence (secondes) : lookback « connu » pour `log_new_signature`, durée totale
    /// des buckets pour `anomaly`. Défaut : 24 h (new_signature), 30×`window_secs` (anomaly).
    #[serde(default)]
    pub baseline_secs: Option<u64>,

    /// Fenêtre glissante (secondes). Requis sauf pour `composite` (chaque sous-condition a la sienne).
    #[serde(default)]
    pub window_secs: u64,
    /// Comparateur valeur ↔ seuil. Défaut `gt`. Inutilisé par `composite` (cf. `op`).
    #[serde(default)]
    pub comparator: Comparator,
    /// Seuil numérique. Défaut 0. Inutilisé par `composite`.
    #[serde(default)]
    pub threshold: f64,
    /// Durée minimale entre deux notifications pour cette règle.
    #[serde(default)]
    pub cooldown_secs: u64,
    /// Sévérité de l'alerte émise (libre : info/warning/critical…).
    #[serde(default = "default_severity")]
    pub severity: String,

    /// Pour `log_group_count` / `log_new_signature` : clé de regroupement. Colonne (`body`,
    /// `service_name`, `severity_text`, `trace_id`) ou `attr:<clé>` (attribut). Défaut `body`.
    #[serde(default)]
    pub group_by: Option<String>,

    /// Pour `composite` : opérateur de combinaison (`all` = ET, défaut ; `any` = OU).
    #[serde(default)]
    pub op: Option<BoolOp>,
    /// Pour `composite` : sous-conditions (règles scalaires ; `name`/`actions`/`cooldown` ignorés).
    #[serde(default)]
    pub conditions: Vec<Rule>,

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
    /// Valide une règle de premier niveau (exige un `name`).
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            anyhow::bail!("règle sans nom");
        }
        self.validate_inner(false)
    }

    /// Cœur de validation. `as_condition` = sous-condition d'un `composite` (pas de `name`/actions,
    /// kinds non scalaires interdits).
    fn validate_inner(&self, as_condition: bool) -> Result<()> {
        match self.kind {
            RuleKind::Composite => {
                if as_condition {
                    bail!("règle '{}': composite imbriqué interdit", self.name);
                }
                if self.conditions.is_empty() {
                    bail!("règle '{}': composite sans conditions", self.name);
                }
                for c in &self.conditions {
                    c.validate_inner(true)?;
                }
            }
            _ => {
                if self.window_secs == 0 {
                    bail!("règle '{}': window_secs doit être > 0", self.name);
                }
                match self.kind {
                    RuleKind::MetricThreshold => {
                        if self.metric_name.as_deref().unwrap_or("").is_empty() {
                            bail!("règle '{}': metric_threshold exige metric_name", self.name);
                        }
                        if self.agg.is_none() {
                            bail!(
                                "règle '{}': metric_threshold exige agg (avg|max|min|sum|count|last|p50..p99)",
                                self.name
                            );
                        }
                    }
                    RuleKind::ErrorRatio => {
                        if !matches!(self.source, Source::Logs | Source::Spans) {
                            bail!(
                                "règle '{}': error_ratio exige source=logs ou source=spans",
                                self.name
                            );
                        }
                    }
                    RuleKind::LogGroupCount | RuleKind::LogNewSignature => {
                        if as_condition {
                            bail!(
                                "règle '{}': {:?} ne peut pas être une sous-condition (non scalaire)",
                                self.name,
                                self.kind
                            );
                        }
                        self.group_expr()?;
                    }
                    _ => {}
                }
            }
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
    fn parses_standard_kinds_and_sources() {
        let raw = r#"{ "rules": [
            { "name":"heartbeat", "kind":"telemetry_count", "source":"metrics", "service":"api",
              "window_secs":300, "comparator":"lte", "threshold":0 },
            { "name":"taux erreur", "kind":"error_ratio", "source":"spans", "service":"api",
              "min_count":50, "window_secs":300, "comparator":"gt", "threshold":0.05 },
            { "name":"p95 checkout", "kind":"span_duration", "agg":"p95", "operation":"checkout",
              "window_secs":300, "comparator":"gt", "threshold":2000 },
            { "name":"pic erreurs", "kind":"relative_change", "source":"logs", "severity_min":17,
              "window_secs":300, "comparator":"gt", "threshold":3 },
            { "name":"p99 latence", "kind":"metric_threshold", "metric_name":"http.server.duration",
              "agg":"p99", "window_secs":300, "comparator":"gt", "threshold":900 }
        ] }"#;
        let rules = parse_rules(raw).unwrap();
        assert_eq!(rules.len(), 5);
        assert_eq!(rules[0].kind, RuleKind::TelemetryCount);
        assert_eq!(rules[0].source, Source::Metrics);
        assert_eq!(rules[1].kind, RuleKind::ErrorRatio);
        assert_eq!(rules[1].min_count, 50);
        assert_eq!(rules[2].agg, Some(Agg::P95));
        assert_eq!(rules[3].kind, RuleKind::RelativeChange);
        assert_eq!(rules[4].agg, Some(Agg::P99));
        assert_eq!(rules[4].agg.unwrap().percentile(), Some(0.99));
    }

    #[test]
    fn error_ratio_rejects_non_log_span_source() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"error_ratio", "source":"metrics",
              "window_secs":60, "comparator":"gt", "threshold":0.1 }
        ] }"#;
        assert!(parse_rules(raw).is_err());
    }

    #[test]
    fn source_defaults_to_logs() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"telemetry_count",
              "window_secs":60, "comparator":"lte", "threshold":0 }
        ] }"#;
        let rules = parse_rules(raw).unwrap();
        assert_eq!(rules[0].source, Source::Logs);
    }

    #[test]
    fn parses_composite_and_advanced_kinds() {
        let raw = r#"{ "rules": [
            { "name":"incident", "kind":"composite", "op":"all", "conditions":[
                { "kind":"error_ratio", "source":"spans", "service":"api", "min_count":50,
                  "window_secs":300, "comparator":"gt", "threshold":0.05 },
                { "kind":"span_duration", "agg":"p95", "service":"api",
                  "window_secs":300, "comparator":"gt", "threshold":2000 }
            ] },
            { "name":"nouvelle erreur", "kind":"log_new_signature", "group_by":"body",
              "severity_min":17, "baseline_secs":86400, "window_secs":300,
              "comparator":"gte", "threshold":1 },
            { "name":"anomalie volume", "kind":"anomaly", "source":"logs", "severity_min":17,
              "baseline_secs":18000, "window_secs":300, "comparator":"gt", "threshold":3 }
        ] }"#;
        let rules = parse_rules(raw).unwrap();
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].kind, RuleKind::Composite);
        assert_eq!(rules[0].op, Some(BoolOp::All));
        assert_eq!(rules[0].conditions.len(), 2);
        assert_eq!(rules[1].kind, RuleKind::LogNewSignature);
        assert_eq!(rules[1].baseline_secs, Some(86_400));
        assert_eq!(rules[2].kind, RuleKind::Anomaly);
    }

    #[test]
    fn rejects_nested_composite() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"composite", "op":"any", "conditions":[
                { "kind":"composite", "conditions":[] }
            ] }
        ] }"#;
        assert!(parse_rules(raw).is_err());
    }

    #[test]
    fn rejects_grouped_kind_as_condition() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"composite", "conditions":[
                { "kind":"log_group_count", "group_by":"body",
                  "window_secs":60, "comparator":"gte", "threshold":5 }
            ] }
        ] }"#;
        assert!(
            parse_rules(raw).is_err(),
            "un kind groupé ne peut pas être une sous-condition"
        );
    }

    #[test]
    fn rejects_empty_composite() {
        let raw = r#"{ "rules": [
            { "name":"x", "kind":"composite", "conditions":[] }
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
