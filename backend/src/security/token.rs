//! Vérification du token d'ingestion (cf. docs/CONTRACT.md §4, cahier §7.3).
//!
//! Signature **asymétrique uniquement** (EdDSA / RS256). Le service d'ingestion ne détient
//! que la (les) **clé(s) publique(s)** : il peut *vérifier* un token mais jamais en *forger*.
//! `none` et les algorithmes symétriques sont rejetés.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

use crate::config::{KeySource, TokenConfig};

/// Identité de confiance extraite d'un token vérifié.
#[derive(Debug, Clone)]
pub struct VerifiedToken {
    pub actor_id: String,
    pub session_id: String,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    actor_id: String,
    session_id: String,
    #[serde(default)]
    tenant_id: Option<String>,
    // `iat` requis par le contrat : sa présence est imposée par la désérialisation.
    #[allow(dead_code)]
    iat: i64,
}

struct KeyEntry {
    kid: Option<String>,
    alg: Algorithm,
    key: DecodingKey,
}

pub struct TokenVerifier {
    enabled: bool,
    algorithms: Vec<Algorithm>,
    issuer: Option<String>,
    audience: Option<String>,
    leeway: u64,
    keys: RwLock<Arc<Vec<KeyEntry>>>,
    jwks: Option<JwksSource>,
}

struct JwksSource {
    url: String,
    refresh: Duration,
    http: reqwest::Client,
}

impl TokenVerifier {
    /// Construit le vérificateur, en chargeant les clés (fetch initial du JWKS si applicable).
    pub async fn new(cfg: &TokenConfig) -> Result<Arc<Self>> {
        let (keys, jwks) = match &cfg.key_source {
            None => (Vec::new(), None),
            Some(KeySource::Pem { pem, alg, kid }) => {
                (vec![build_pem_key(pem, *alg, kid.clone())?], None)
            }
            Some(KeySource::Jwks { url }) => {
                let http = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()
                    .context("client HTTP JWKS")?;
                let keys = fetch_jwks(&http, url).await?;
                (
                    keys,
                    Some(JwksSource {
                        url: url.clone(),
                        refresh: cfg.jwks_refresh,
                        http,
                    }),
                )
            }
        };

        Ok(Arc::new(Self {
            enabled: cfg.enabled,
            algorithms: cfg.algorithms.clone(),
            issuer: cfg.issuer.clone(),
            audience: cfg.audience.clone(),
            leeway: cfg.leeway_secs,
            keys: RwLock::new(Arc::new(keys)),
            jwks,
        }))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Vérifie un token et retourne l'identité de confiance, ou un message d'erreur (→ 401).
    pub fn verify(&self, token: &str) -> std::result::Result<VerifiedToken, String> {
        let header = decode_header(token).map_err(|e| format!("en-tête JWT invalide: {e}"))?;

        // `none` est déjà rejeté par decode_header ; on impose en plus la liste autorisée.
        if !self.algorithms.contains(&header.alg) {
            return Err(format!("algorithme non autorisé: {:?}", header.alg));
        }

        let keys = self.keys.read().expect("verrou clés empoisonné").clone();
        let entry = select_key(&keys, header.kid.as_deref(), header.alg)
            .ok_or_else(|| "clé de signature inconnue".to_string())?;

        let mut validation = Validation::new(header.alg);
        validation.leeway = self.leeway;
        validation.validate_exp = true;
        validation.set_required_spec_claims(&["exp"]);
        if let Some(iss) = &self.issuer {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = &self.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let data = decode::<Claims>(token, &entry.key, &validation).map_err(map_jwt_err)?;
        let c = data.claims;
        if c.actor_id.trim().is_empty() || c.session_id.trim().is_empty() {
            return Err("claims requis manquants (actor_id/session_id)".to_string());
        }
        Ok(VerifiedToken {
            actor_id: c.actor_id,
            session_id: c.session_id,
            tenant_id: c.tenant_id,
        })
    }

    /// Tâche de fond : rafraîchit périodiquement le JWKS (rotation des clés sans redéploiement).
    pub fn spawn_refresh(self: &Arc<Self>) {
        let Some(jwks) = self.jwks.as_ref() else {
            return;
        };
        let this = Arc::clone(self);
        let url = jwks.url.clone();
        let refresh = jwks.refresh;
        let http = jwks.http.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(refresh);
            ticker.tick().await; // consomme le tick immédiat
            loop {
                ticker.tick().await;
                match fetch_jwks(&http, &url).await {
                    Ok(keys) => {
                        *this.keys.write().expect("verrou clés empoisonné") = Arc::new(keys);
                        tracing::info!("JWKS rafraîchi");
                    }
                    Err(e) => tracing::warn!(error = %e, "échec du rafraîchissement JWKS"),
                }
            }
        });
    }
}

fn select_key<'a>(keys: &'a [KeyEntry], kid: Option<&str>, alg: Algorithm) -> Option<&'a KeyEntry> {
    if let Some(kid) = kid {
        // kid fourni : exiger une correspondance exacte (pas de repli silencieux).
        return keys
            .iter()
            .find(|k| k.kid.as_deref() == Some(kid) && k.alg == alg);
    }
    keys.iter().find(|k| k.alg == alg)
}

fn build_pem_key(pem: &str, alg: Algorithm, kid: Option<String>) -> Result<KeyEntry> {
    let key = match alg {
        Algorithm::EdDSA => DecodingKey::from_ed_pem(pem.as_bytes())
            .context("clé publique Ed25519 (PEM) invalide")?,
        Algorithm::RS256 => {
            DecodingKey::from_rsa_pem(pem.as_bytes()).context("clé publique RSA (PEM) invalide")?
        }
        other => bail!("algorithme non supporté pour une clé PEM: {other:?}"),
    };
    Ok(KeyEntry { kid, alg, key })
}

async fn fetch_jwks(http: &reqwest::Client, url: &str) -> Result<Vec<KeyEntry>> {
    let set: JwkSet = http
        .get(url)
        .send()
        .await
        .with_context(|| format!("requête JWKS {url}"))?
        .error_for_status()
        .context("réponse JWKS non 2xx")?
        .json()
        .await
        .context("JWKS JSON invalide")?;

    let mut entries = Vec::new();
    for jwk in &set.keys {
        let alg = match &jwk.algorithm {
            AlgorithmParameters::OctetKeyPair(_) => Algorithm::EdDSA,
            AlgorithmParameters::RSA(_) => Algorithm::RS256,
            _ => continue, // on ignore les clés symétriques / non supportées
        };
        let key = DecodingKey::from_jwk(jwk).context("clé JWKS invalide")?;
        entries.push(KeyEntry {
            kid: jwk.common.key_id.clone(),
            alg,
            key,
        });
    }
    if entries.is_empty() {
        bail!("JWKS ne contient aucune clé asymétrique exploitable");
    }
    Ok(entries)
}

fn map_jwt_err(e: jsonwebtoken::errors::Error) -> String {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => "token expiré".to_string(),
        ErrorKind::InvalidSignature => "signature invalide".to_string(),
        ErrorKind::InvalidIssuer => "issuer invalide".to_string(),
        ErrorKind::InvalidAudience => "audience invalide".to_string(),
        ErrorKind::MissingRequiredClaim(c) => format!("claim requis manquant: {c}"),
        ErrorKind::InvalidToken => "token malformé".to_string(),
        _ => "token invalide".to_string(),
    }
}
