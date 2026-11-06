//! A connect that succeeds proves nothing: the default modes fall back to plain text.
//! These tests ask the server whether the session is encrypted.

use sqlx::Row;
use yado_core::Store;

mod common;
use common::*;

async fn mysql_cipher(store: &Store) -> String {
    sqlx::query("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get(1)
}

async fn postgres_ssl(store: &Store) -> bool {
    sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

async fn migrate_and_start(store: &Store) {
    store.migrate().await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    start(store, &workflow(&id, LONG)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_required_session_is_encrypted() {
    let db = TestDb::create(Backend::MySql).await;
    let store = Store::connect(&format!("{}?ssl-mode=REQUIRED", db.url))
        .await
        .unwrap();
    migrate_and_start(&store).await;
    let cipher = mysql_cipher(&store).await;
    println!("mysql Ssl_cipher: {cipher}");
    assert!(!cipher.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_disabled_session_has_no_cipher() {
    let db = TestDb::create(Backend::MySql).await;
    let store = Store::connect(&format!("{}?ssl-mode=DISABLED", db.url))
        .await
        .unwrap();
    assert_eq!(mysql_cipher(&store).await, "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs YADO_TEST_PG_TLS_URL: a Postgres server with ssl=on, and sslmode=require in the URL"]
async fn postgres_require_session_is_encrypted() {
    let url = std::env::var("YADO_TEST_PG_TLS_URL").expect("YADO_TEST_PG_TLS_URL is not set");
    assert!(url.contains("sslmode=require"), "{url} must require TLS");
    let store = Store::connect(&url).await.unwrap();
    migrate_and_start(&store).await;
    let ssl = postgres_ssl(&store).await;
    println!("postgres pg_stat_ssl.ssl: {ssl}");
    assert!(ssl);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_require_refuses_a_server_without_tls() {
    let url = postgres_admin_url();
    let plain = Store::connect(&format!("{url}?sslmode=disable"))
        .await
        .unwrap();
    assert!(!postgres_ssl(&plain).await);

    let error = Store::connect(&format!("{url}?sslmode=require"))
        .await
        .expect_err("sslmode=require must not fall back to plain text");
    println!("postgres sslmode=require on the plain server: {error}");
    assert!(error.to_string().contains("server does not support TLS"));
}
