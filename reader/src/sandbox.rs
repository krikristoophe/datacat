//! Garde-fous du moteur de lecture froid (S-6) : confiner DataFusion au **seul** bucket S3
//! configuré et n'autoriser que des requêtes en **lecture seule**.
//!
//! Le `reader` exécute du SQL arbitraire. Sur un `SessionContext` par défaut, les fonctions de
//! table `read_csv` / `read_parquet` / `read_json` (et `CREATE EXTERNAL TABLE … LOCATION` /
//! `COPY … TO`) peuvent lire ou écrire le système de fichiers local — p.ex.
//! `SELECT * FROM read_csv('/etc/passwd')` exfiltre un fichier hôte. Deux garde-fous en défense
//! en profondeur :
//!
//! 1. [`S3OnlyObjectStoreRegistry`] — **tout** accès fichier de DataFusion (fonctions de table,
//!    `LOCATION`, `COPY`, inférence de schéma) passe par le registre d'object stores. On
//!    n'enregistre **jamais** le store local `file://` : seul le bucket S3 configuré est résolu,
//!    tout le reste (local, autre bucket, http…) est refusé. C'est le correctif de fond, en un
//!    seul point, indépendant de la liste des fonctions de table de DataFusion.
//! 2. [`ensure_read_only`] — rejet de tout plan logique qui n'est pas une lecture (DDL, DML,
//!    `COPY`, statements), par principe de moindre privilège.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::logical_expr::LogicalPlan;
use object_store::ObjectStore;
use url::{Position, Url};

/// Clé d'enregistrement d'un object store : `scheme://host:port` (sans les éventuels
/// identifiants). Réplique la convention du registre par défaut de DataFusion.
fn url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[Position::BeforeHost..Position::AfterPort],
    )
}

/// Registre d'object stores qui ne résout que les stores explicitement enregistrés (le bucket S3
/// configuré). Contrairement au registre par défaut de DataFusion, il n'enregistre **pas** le
/// store local `file://` : toute URL non enregistrée est refusée.
pub struct S3OnlyObjectStoreRegistry {
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl S3OnlyObjectStoreRegistry {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for S3OnlyObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for S3OnlyObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<String> = self
            .stores
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        f.debug_struct("S3OnlyObjectStoreRegistry")
            .field("allowed", &keys)
            .finish()
    }
}

impl ObjectStoreRegistry for S3OnlyObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mut map = self.stores.write().unwrap_or_else(|e| e.into_inner());
        map.insert(url_key(url), store)
    }

    fn get_store(&self, url: &Url) -> DfResult<Arc<dyn ObjectStore>> {
        let key = url_key(url);
        let map = self.stores.read().unwrap_or_else(|e| e.into_inner());
        map.get(&key).map(Arc::clone).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "object store access denied by cold-reader sandbox: only the configured S3 bucket \
                 is permitted (requested '{key}'). Local-file (file://) and other-bucket access \
                 are blocked."
            ))
        })
    }
}

/// Rejette les plans qui ne sont pas en lecture seule (DDL / DML / `COPY` / statements).
/// `SELECT`, `EXPLAIN`, `ANALYZE` et les CTE restent autorisés.
pub fn ensure_read_only(plan: &LogicalPlan) -> DfResult<()> {
    match plan {
        LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::Copy(_) => {
            Err(DataFusionError::Execution(
                "cold reader is read-only: DDL, DML and COPY statements are not permitted".into(),
            ))
        }
        LogicalPlan::Statement(_) => Err(DataFusionError::Execution(
            "cold reader is read-only: this statement type is not permitted".into(),
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_key_strips_path_and_credentials() {
        let u = Url::parse("s3://my-bucket/events/date=2024-06-15/part.parquet").unwrap();
        assert_eq!(url_key(&u), "s3://my-bucket");
        let f = Url::parse("file:///etc/passwd").unwrap();
        assert_eq!(url_key(&f), "file://");
    }

    #[test]
    fn registry_denies_local_file_and_unknown_bucket() {
        let reg = S3OnlyObjectStoreRegistry::new();
        // Aucun store local n'est enregistré : file:// est refusé.
        let file = Url::parse("file:///etc/passwd").unwrap();
        assert!(reg.get_store(&file).is_err(), "file:// doit être refusé");

        // Un bucket non enregistré est refusé.
        let other = Url::parse("s3://other-bucket/x").unwrap();
        assert!(reg.get_store(&other).is_err(), "autre bucket refusé");
    }

    #[test]
    fn registry_allows_registered_bucket() {
        let reg = S3OnlyObjectStoreRegistry::new();
        let bucket = Url::parse("s3://datacat-cold/").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        reg.register_store(&bucket, store);

        // Une URL d'objet dans ce bucket est résolue.
        let obj = Url::parse("s3://datacat-cold/events/part.parquet").unwrap();
        assert!(reg.get_store(&obj).is_ok(), "bucket enregistré autorisé");
        // Le store local reste refusé.
        let file = Url::parse("file:///etc/passwd").unwrap();
        assert!(reg.get_store(&file).is_err());
    }

    /// Construit un `SessionContext` sandboxé identique à celui de `ColdReader::new`.
    fn sandboxed_ctx() -> datafusion::prelude::SessionContext {
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};
        let runtime = RuntimeEnvBuilder::new()
            .with_object_store_registry(Arc::new(S3OnlyObjectStoreRegistry::new()))
            .build_arc()
            .unwrap();
        SessionContext::new_with_config_rt(SessionConfig::default(), runtime)
    }

    #[tokio::test]
    async fn read_csv_local_file_is_denied_end_to_end() {
        let ctx = sandboxed_ctx();
        // L'inférence de schéma de read_csv passe par le registre → file:// refusé.
        let planned = ctx.sql("SELECT * FROM read_csv('/etc/passwd')").await;
        let denied = match planned {
            Err(_) => true,
            Ok(df) => df.collect().await.is_err(),
        };
        assert!(denied, "read_csv d'un fichier local hôte doit être refusé");
    }

    #[tokio::test]
    async fn read_parquet_local_file_is_denied_end_to_end() {
        let ctx = sandboxed_ctx();
        let planned = ctx
            .sql("SELECT * FROM read_parquet('file:///etc/hosts')")
            .await;
        let denied = match planned {
            Err(_) => true,
            Ok(df) => df.collect().await.is_err(),
        };
        assert!(
            denied,
            "read_parquet d'un fichier local hôte doit être refusé"
        );
    }

    #[tokio::test]
    async fn copy_to_is_rejected_as_read_only() {
        let ctx = sandboxed_ctx();
        // COPY … TO doit être rejeté par ensure_read_only (avant toute écriture).
        let df = ctx
            .sql("COPY (SELECT 1 AS x) TO 'file:///tmp/datacat-x.csv' STORED AS CSV")
            .await
            .expect("COPY doit produire un plan");
        assert!(
            ensure_read_only(df.logical_plan()).is_err(),
            "COPY doit être refusé (lecture seule)"
        );
    }

    #[tokio::test]
    async fn select_is_allowed_by_read_only_guard() {
        let ctx = sandboxed_ctx();
        let df = ctx.sql("SELECT 1 AS x").await.unwrap();
        assert!(
            ensure_read_only(df.logical_plan()).is_ok(),
            "un SELECT pur doit être autorisé"
        );
    }
}
