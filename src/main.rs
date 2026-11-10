//! Binary entrypoint for the `rust-balancer` L7 HTTP Load Balancer.
//!
//! Uses `anyhow` strictly at the binary boundary (Chapter 4, §4.4 of Rust Best Practices),
//! delegating all core logic to the `rust_balancer` library.

// std
use std::env;
use std::fs;
use std::sync::Arc;

// external crates
use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

// crate
use rust_balancer::config::BalancerBuilder;
use rust_balancer::ddns::{DdnsState, DdnsUpdater, HttpDdnsProvider};
use rust_balancer::health::{HealthCheckRunner, HttpHealthProbe};
use rust_balancer::proxy::{build_router, BalancerAppState, HttpUpstreamForwarder};
use rust_balancer::state::ServiceRegistry;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("rust_balancer=info,axum=info")),
        )
        .init();

    let config_path =
        env::var("BALANCER_CONFIG").unwrap_or_else(|_| "config/balancer.toml".to_string());

    let raw_toml = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read configuration file at `{config_path}`"))?;

    let validated = BalancerBuilder::from_toml(&raw_toml)
        .and_then(BalancerBuilder::validate)
        .context("Configuration validation failed")?;

    let listen_addr = validated.config().listen_addr.clone();
    let ddns_config = validated.config().ddns.clone();
    let registry = ServiceRegistry::from_validated(&validated);
    let ddns_state = Arc::new(DdnsState::new(ddns_config));

    let shared_http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .build()
        .context("Failed to initialize shared HTTP client")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let health_runner = HealthCheckRunner::new(
        Arc::clone(&registry),
        HttpHealthProbe::with_client(shared_http_client.clone()),
    );
    let health_handle = tokio::spawn(health_runner.run_until_shutdown(shutdown_rx.clone()));

    let ddns_updater = DdnsUpdater::new(
        Arc::clone(&registry),
        Arc::clone(&ddns_state),
        HttpDdnsProvider::with_client(shared_http_client.clone()),
    );
    let ddns_handle = tokio::spawn(ddns_updater.run_until_shutdown(shutdown_rx));

    let app_state = BalancerAppState::with_ddns(
        Arc::clone(&registry),
        Arc::clone(&ddns_state),
        HttpUpstreamForwarder::with_client(shared_http_client),
    );
    let app = build_router(app_state);

    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("Failed to bind TCP listener on `{listen_addr}`"))?;

    tracing::info!(
        listen_addr = %listen_addr,
        services = validated.config().services.len(),
        ddns_enabled = validated.config().ddns.enabled,
        "L7 Rust Load Balancer started and ready to accept connections"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx))
        .await
        .context("L7 Load Balancer HTTP server terminated with an error")?;

    let _ = health_handle.await;
    let _ = ddns_handle.await;
    Ok(())
}

async fn shutdown_signal(shutdown_tx: watch::Sender<bool>) {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "Failed to listen for Ctrl+C shutdown signal");
    }
    tracing::info!("Shutdown signal received, stopping L7 Load Balancer gracefully...");
    let _ = shutdown_tx.send(true);
}
