//! Datacat remote companion agent — the remote half of a bidirectional dead-man's-switch.
//!
//! This crate runs on a remote node and periodically POSTs a heartbeat to a Datacat **main**
//! instance (`POST {main_url}/v1/heartbeat`, `Authorization: Bearer <token>`, JSON `{"id": …}`).
//! When main stays unreachable for `failure_threshold` consecutive attempts, the agent raises its
//! **own** alert (Slack or webhook) — because a down main cannot alert about itself. The main side
//! independently alerts when a companion goes silent; that half lives in the backend.
//!
//! Layout:
//! - [`config`] — TOML config with `${ENV}` secret expansion and duration parsing;
//! - [`alert`] — the self-alert sinks (Slack / webhook) behind an object-safe trait;
//! - [`agent`] — the heartbeat sender plus the testable failure→firing→resolved state machine.

#![forbid(unsafe_code)]

pub mod agent;
pub mod alert;
pub mod config;

pub use agent::{Agent, Beat, StateMachine};
pub use alert::{AlertSink, AlertState, SelfAlert};
pub use config::Config;
