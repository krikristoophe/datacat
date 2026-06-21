//! Outil de DÉVELOPPEMENT : signe un token d'ingestion à partir d'une clé privée locale.
//!
//! ⚠️ Jamais déployé en production. L'émission du token relève des backends consommateurs
//! (hors scope, cf. cahier §7.3.1) ; cet outil sert à tester l'ingestion et à illustrer le
//! contrat de token (docs/token-contract.md).
//!
//! Exemple :
//!   mint-dev-token --key keys/ed25519_private.pem --alg EdDSA \
//!     --actor user-123 --session sess-abc --tenant clinic-42 --ttl 600 \
//!     --aud datacat-ingest --iss swappy-backend --kid 2026-06-key-1

#![forbid(unsafe_code)]

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;

#[derive(Serialize)]
struct Claims {
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<String>,
    sub: String,
    actor_id: String,
    session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    iat: i64,
    exp: i64,
}

fn main() -> Result<()> {
    let args = parse_args()?;

    let key_path = args
        .get("key")
        .context("--key <fichier PEM privé> requis")?;
    let alg = match args.get("alg").map(String::as_str).unwrap_or("EdDSA") {
        "EdDSA" | "Ed25519" => Algorithm::EdDSA,
        "RS256" => Algorithm::RS256,
        other => bail!("--alg non supporté: {other} (EdDSA ou RS256)"),
    };

    let pem = std::fs::read(key_path).with_context(|| format!("lecture de {key_path}"))?;
    let key = match alg {
        Algorithm::EdDSA => {
            EncodingKey::from_ed_pem(&pem).context("clé privée Ed25519 invalide")?
        }
        Algorithm::RS256 => EncodingKey::from_rsa_pem(&pem).context("clé privée RSA invalide")?,
        _ => unreachable!(),
    };

    let actor = args.get("actor").context("--actor requis")?.clone();
    let session = args.get("session").context("--session requis")?.clone();
    let ttl: i64 = args
        .get("ttl")
        .map(|s| s.parse())
        .transpose()
        .context("--ttl invalide")?
        .unwrap_or(600);

    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        iss: args.get("iss").cloned(),
        aud: args.get("aud").cloned(),
        sub: args.get("sub").cloned().unwrap_or_else(|| actor.clone()),
        actor_id: actor,
        session_id: session,
        tenant_id: args.get("tenant").cloned(),
        iat: now,
        exp: now + ttl,
    };

    let mut header = Header::new(alg);
    header.kid = args.get("kid").cloned();

    let token = encode(&header, &claims, &key).context("signature du token")?;
    println!("{token}");
    Ok(())
}

/// Mini-parseur d'arguments `--clé valeur`.
fn parse_args() -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let Some(key) = arg.strip_prefix("--") else {
            bail!("argument inattendu: {arg}");
        };
        let value = it
            .next()
            .with_context(|| format!("valeur manquante pour --{key}"))?;
        map.insert(key.to_string(), value);
    }
    Ok(map)
}
