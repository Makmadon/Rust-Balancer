//! Active health check subsystem with static dispatch over [`HealthProbe`].
//!
//! Follows Chapter 6 of the Rust Best Practices handbook: [`HealthCheckRunner<P>`] uses
//! generic static dispatch (`P: HealthProbe`) so that production probes are inlined with
//! zero vtable overhead while unit tests can inject deterministic in-memory probes.

// std
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

// external crates
use tokio::sync::watch;

// crate
use crate::error::BalancerError;
use crate::state::ServiceRegistry;

/// Trait abstracting an active health check probe against a single backend endpoint.
pub trait HealthProbe: Send + Sync + 'static {
    /// Probes `probe_url` within `timeout` and returns `Ok(true)` if the backend is healthy,
    /// `Ok(false)` if the backend responded with a non-2xx status code, or `Err` on transport
    /// failure.
    fn probe(
        &self,
        probe_url: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<bool, BalancerError>> + Send;
}

/// Production HTTP health probe backed by a shared [`reqwest::Client`] connection pool.
#[derive(Debug, Clone)]
pub struct HttpHealthProbe {
    client: reqwest::Client,
}

impl Default for HttpHealthProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpHealthProbe {
    /// Creates a new [`HttpHealthProbe`] with a shared HTTP client.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Creates a [`HttpHealthProbe`] reusing an existing [`reqwest::Client`].
    #[must_use]
    pub const fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl HealthProbe for HttpHealthProbe {
    async fn probe(&self, probe_url: &str, timeout: Duration) -> Result<bool, BalancerError> {
        let response = self
            .client
            .get(probe_url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|err| BalancerError::HealthProbeFailure {
                backend_url: probe_url.to_string(),
                reason: err.to_string(),
            })?;

        Ok(response.status().is_success())
    }
}

/// Periodic active health checker parameterized over [`HealthProbe`] (static dispatch).
#[derive(Debug, Clone)]
pub struct HealthCheckRunner<P: HealthProbe = HttpHealthProbe> {
    registry: Arc<ServiceRegistry>,
    probe: Arc<P>,
}

impl<P: HealthProbe> HealthCheckRunner<P> {
    /// Constructs a new [`HealthCheckRunner`] for the given [`ServiceRegistry`] and [`HealthProbe`].
    #[must_use]
    pub fn new(registry: Arc<ServiceRegistry>, probe: P) -> Self {
        Self {
            registry,
            probe: Arc::new(probe),
        }
    }

    /// Executes a single health check sweep across all services and backends in the registry.
    ///
    /// Returns the total number of backends probed during the sweep.
    pub async fn run_sweep_once(&self) -> usize {
        let hc_config = self.registry.health_check_config();
        let services = self.registry.all_services().await;
        let mut probed_count = 0usize;

        for pool in services {
            let health_path = pool.health_check_path();
            let backends = pool.backends().await;

            for node in backends {
                probed_count += 1;
                let probe_url = format!("{}{health_path}", node.url());
                let result = self.probe.probe(&probe_url, hc_config.timeout()).await;

                match result {
                    Ok(true) => {
                        if node.record_success(hc_config.healthy_threshold) {
                            tracing::info!(
                                service = pool.name(),
                                backend = node.url(),
                                "Backend recovered and is now marked HEALTHY"
                            );
                        }
                    }
                    Ok(false) => {
                        if node.record_failure(hc_config.unhealthy_threshold) {
                            tracing::warn!(
                                service = pool.name(),
                                backend = node.url(),
                                "Backend returned non-2xx status and is now marked UNHEALTHY"
                            );
                        }
                    }
                    Err(err) => {
                        if node.record_failure(hc_config.unhealthy_threshold) {
                            tracing::warn!(
                                service = pool.name(),
                                backend = node.url(),
                                error = %err,
                                "Backend failed health probe and is now marked UNHEALTHY"
                            );
                        }
                    }
                }
            }
        }

        probed_count
    }

    /// Runs the active health check loop periodically until `shutdown_rx` signals termination.
    pub async fn run_until_shutdown(self, mut shutdown_rx: watch::Receiver<bool>) {
        let interval_duration = self.registry.health_check_config().interval();
        let mut ticker = tokio::time::interval(interval_duration);
        // The first tick completes immediately; consume it so we wait one interval before probing,
        // or run an initial sweep right away.
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.run_sweep_once().await;
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        tracing::info!("Shutting down active HealthCheckRunner");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::config::{
        BackendConfig, BalancerBuilder, BalancerConfig, HealthCheckConfig, ServiceConfig,
        StrategyKind,
    };

    #[derive(Debug, Default)]
    struct MockHealthProbe {
        responses: Mutex<HashMap<String, Result<bool, String>>>,
    }

    impl MockHealthProbe {
        fn set_response(&self, url: &str, outcome: Result<bool, &str>) {
            let mut guard = self.responses.lock().unwrap();
            guard.insert(
                url.to_string(),
                outcome.map_err(std::string::ToString::to_string),
            );
        }
    }

    impl HealthProbe for MockHealthProbe {
        async fn probe(&self, probe_url: &str, _timeout: Duration) -> Result<bool, BalancerError> {
            let outcome = {
                let guard = self.responses.lock().unwrap();
                guard.get(probe_url).cloned().unwrap_or(Ok(true))
            };
            outcome.map_err(|reason| BalancerError::HealthProbeFailure {
                backend_url: probe_url.to_string(),
                reason,
            })
        }
    }

    fn test_registry() -> Arc<ServiceRegistry> {
        let config = BalancerConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            health_check: HealthCheckConfig {
                interval_ms: 100,
                timeout_ms: 50,
                unhealthy_threshold: 2,
                healthy_threshold: 2,
            },
            ddns: crate::config::DdnsConfig::default(),
            services: vec![ServiceConfig {
                name: "orders".to_string(),
                route_prefix: "/orders".to_string(),
                strip_prefix: true,
                strategy: StrategyKind::RoundRobin,
                health_check_path: "/health".to_string(),
                backends: vec![
                    BackendConfig {
                        url: "http://127.0.0.1:4001".to_string(),
                        weight: 1,
                    },
                    BackendConfig {
                        url: "http://127.0.0.1:4002".to_string(),
                        weight: 1,
                    },
                ],
            }],
        };
        let validated = BalancerBuilder::new(config).validate().unwrap();
        ServiceRegistry::from_validated(&validated)
    }

    mod sweep {
        use super::*;

        #[tokio::test]
        async fn marks_failing_backend_unhealthy_after_threshold_sweeps() {
            let registry = test_registry();
            let probe = MockHealthProbe::default();
            probe.set_response("http://127.0.0.1:4001/health", Err("connection refused"));
            probe.set_response("http://127.0.0.1:4002/health", Ok(true));

            let runner = HealthCheckRunner::new(Arc::clone(&registry), probe);

            runner.run_sweep_once().await;
            runner.run_sweep_once().await;

            let pool = registry.get_service("orders").await.unwrap();
            let backends = pool.backends().await;
            assert_eq!(
                (backends[0].is_healthy(), backends[1].is_healthy()),
                (false, true)
            );
        }

        #[tokio::test]
        async fn recovers_unhealthy_backend_after_consecutive_healthy_sweeps() {
            let registry = test_registry();
            let pool = registry.get_service("orders").await.unwrap();
            let backends = pool.backends().await;
            backends[0].set_healthy(false);

            let probe = MockHealthProbe::default();
            probe.set_response("http://127.0.0.1:4001/health", Ok(true));
            let runner = HealthCheckRunner::new(Arc::clone(&registry), probe);

            runner.run_sweep_once().await;
            runner.run_sweep_once().await;

            assert!(backends[0].is_healthy());
        }
    }
}
