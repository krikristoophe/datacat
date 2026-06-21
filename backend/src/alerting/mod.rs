//! Moteur d'alerting : règles déclaratives (JSON), évaluation périodique sur `logs` /
//! `metric_points`, notifications Slack + e-mail.
//!
//! Frontières : le moteur lit la base (pool partagé) mais reste indépendant de l'ingestion.
//! Il est activé uniquement si des règles **et** au moins un notifier sont configurés
//! (cf. `crate::config` et `crate::main`).

pub mod eval;
pub mod notify;
pub mod rules;

pub use eval::{evaluate_once, run_eval_loop, AlertEngineState, RuleState};
pub use notify::{
    Alert, AlertState, DispatchSettings, Dispatcher, EmailConfig, EmailNotifier, Notifier,
    RecordingNotifier, SlackBot, SlackNotifier, WebhookNotifier,
};
pub use rules::{
    load_rules, parse_rules, Action, Agg, BoolOp, Comparator, GroupExpr, Rule, RuleKind, Source,
};
