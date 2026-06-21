//! Tests d'intégration du heartbeat companion (`POST /v1/heartbeat`).

mod common;

use common::*;
use sqlx::PgPool;

#[sqlx::test]
async fn heartbeat_requires_auth_and_records_companion(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("svc", "s", 600);
    let client = reqwest::Client::new();
    let url = format!("{}/v1/heartbeat", app.base_url);

    // Sans token → 401 (auth service-à-service via logs_auth).
    let r = client
        .post(&url)
        .json(&serde_json::json!({ "id": "edge-eu" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // id vide → 400.
    let r = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({ "id": "  " }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // Heartbeat valide → 204.
    let r = client
        .post(&url)
        .bearer_auth(&token)
        .json(&serde_json::json!({ "id": "edge-eu" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);

    // `/stats` (query_auth=None en test) liste le companion enregistré.
    let stats: serde_json::Value = client
        .get(format!("{}/stats", app.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = stats["companions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"edge-eu"), "companions = {:?}", ids);
}
