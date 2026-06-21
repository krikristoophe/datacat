//! Domaine « events produit » : modèle d'event et validation stricte des entrées.

pub mod model;

pub use model::{check_event, EventCheck, IncomingEvent, IngestBody, StoredEvent, StructuralError};
