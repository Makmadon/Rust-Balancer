//! Dynamic DNS (DDNS) coordination and synchronization subsystem.
//!
//! Enables multiple geographically or dynamically addressed `rust-balancer` instances
//! to coordinate with a DDNS provider (`DynDNS2` / `DuckDNS` / No-IP / Cloudflare webhook style):
//! 1. Periodically resolves the instance's public IP address via [`DdnsProvider::resolve_public_ip`].
//! 2. Checks whether this balancer instance has healthy upstream capacity.
//! 3. Publishes an updated DNS record via [`DdnsProvider::update_record`] only when the public IP
//!    changes or requires a heartbeat refresh, storing the state in [`DdnsState`].

// std
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// external crates
use serde::Serialize;
use tokio::sync::{watch, RwLock};

// crate
use crate::config::DdnsConfig;
use crate::error::BalancerError;
use crate::state::ServiceRegistry;

/// Trait abstracting external DDNS IP discovery and DNS record update calls.
///
/// Uses static dispatch (`<D: DdnsProvider>`) in [`DdnsUpdater<D>`] (Chapter 6 of Rust Best Practices).
pub trait DdnsProvider: Send + Sync + 'static {
    /// Resolves the current public IP address of this load balancer instance.
    fn resolve_public_ip(
        &self,
        config: &DdnsConfig,
    ) -> impl Future<Output = Result<String, BalancerError>> + Send;

    /// Publishes `public_ip` for `config.hostname` to the configured DDNS provider.
    fn update_record(
        &self,
        config: &DdnsConfig,
        public_ip: &str,
    ) -> impl Future<Output = Result<(), BalancerError>> + Send;
}

/// Production HTTP-based [`DdnsProvider`] compatible with standard `DynDNS2`/REST webhooks
/// (`GET <update_url>?hostname=<host>&myip=<ip>&instance=<id>`).
#[derive(Debug, Clone)]
pub struct HttpDdnsProvider {
    client: reqwest::Client,
}

impl Default for HttpDdnsProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpDdnsProvider {
    /// Creates a new [`HttpDdnsProvider`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Creates an [`HttpDdnsProvider`] reusing an existing [`reqwest::Client`].
    #[must_use]
    pub const fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl DdnsProvider for HttpDdnsProvider {
    async fn resolve_public_ip(&self, config: &DdnsConfig) -> Result<String, BalancerError> {
        let resp = self
            .client
            .get(&config.ip_check_url)
            .timeout(config.timeout())
            .send()
            .await
            .map_err(|err| BalancerError::DdnsFailure {
                hostname: config.hostname.clone(),
                reason: format!("public IP lookup failed: {err}"),
            })?;

        if !resp.status().is_success() {
            return Err(BalancerError::DdnsFailure {
                hostname: config.hostname.clone(),
                reason: format!("public IP lookup returned HTTP {}", resp.status()),
            });
        }

        let ip = resp
            .text()
            .await
            .map_err(|err| BalancerError::DdnsFailure {
                hostname: config.hostname.clone(),
                reason: format!("failed reading public IP response: {err}"),
            })?
            .trim()
            .to_string();

        if ip.is_empty() {
            return Err(BalancerError::DdnsFailure {
                hostname: config.hostname.clone(),
                reason: "public IP lookup returned an empty string".to_string(),
            });
        }

        Ok(ip)
    }

    async fn update_record(
        &self,
        config: &DdnsConfig,
        public_ip: &str,
    ) -> Result<(), BalancerError> {
        let mut req = self
            .client
            .get(&config.update_url)
            .query(&[
                ("hostname", config.hostname.as_str()),
                ("myip", public_ip),
                ("instance", config.instance_id.as_str()),
            ])
            .timeout(config.timeout());

        if let Some(token) = config.auth_token.as_deref().filter(|t| !t.is_empty()) {
            req = req.bearer_auth(token);
        }

        let resp = req.send().await.map_err(|err| BalancerError::DdnsFailure {
            hostname: config.hostname.clone(),
            reason: format!("DDNS update request failed: {err}"),
        })?;

        if !resp.status().is_success() {
            return Err(BalancerError::DdnsFailure {
                hostname: config.hostname.clone(),
                reason: format!("DDNS provider returned HTTP {}", resp.status()),
            });
        }

        Ok(())
    }
}

/// Shared state tracking DDNS synchronization status for observability and coordination.
///
/// # Examples
///
/// ```
/// use rust_balancer::config::DdnsConfig;
/// use rust_balancer::ddns::DdnsState;
///
/// let rt = tokio::runtime::Runtime::new().unwrap();
/// rt.block_on(async {
///     let state = DdnsState::new(DdnsConfig::default());
///     let snap = state.snapshot().await;
///     assert!(!snap.enabled);
///     assert_eq!(snap.successful_updates, 0);
/// });
/// ```
#[derive(Debug)]
pub struct DdnsState {
    config: DdnsConfig,
    last_published_ip: RwLock<Option<String>>,
    successful_updates: AtomicU64,
    failed_updates: AtomicU64,
}

impl DdnsState {
    /// Creates a new [`DdnsState`] from the given [`DdnsConfig`].
    #[must_use]
    pub fn new(config: DdnsConfig) -> Self {
        Self {
            config,
            last_published_ip: RwLock::new(None),
            successful_updates: AtomicU64::new(0),
            failed_updates: AtomicU64::new(0),
        }
    }

    /// Returns a reference to the [`DdnsConfig`].
    #[must_use]
    pub const fn config(&self) -> &DdnsConfig {
        &self.config
    }

    /// Returns the most recently published public IP, if any.
    pub async fn last_published_ip(&self) -> Option<String> {
        self.last_published_ip.read().await.clone()
    }

    /// Captures a serializable [`DdnsSnapshot`] of the current DDNS state.
    pub async fn snapshot(&self) -> DdnsSnapshot {
        let last_ip = self.last_published_ip.read().await.clone();
        DdnsSnapshot {
            enabled: self.config.enabled,
            instance_id: self.config.instance_id.clone(),
            hostname: self.config.hostname.clone(),
            last_published_ip: last_ip,
            successful_updates: self.successful_updates.load(Ordering::Relaxed),
            failed_updates: self.failed_updates.load(Ordering::Relaxed),
        }
    }
}

/// Serializable status snapshot of the DDNS subsystem exposed on `/_balancer/status`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DdnsSnapshot {
    /// Whether DDNS synchronization is enabled.
    pub enabled: bool,
    /// Unique identifier of this balancer instance.
    pub instance_id: String,
    /// Target DNS hostname coordinated by this instance.
    pub hostname: String,
    /// Last public IP successfully published to the DDNS provider.
    pub last_published_ip: Option<String>,
    /// Count of successful DDNS record updates.
    pub successful_updates: u64,
    /// Count of failed DDNS record update attempts.
    pub failed_updates: u64,
}

/// Periodic DDNS synchronization worker parameterized over [`DdnsProvider`].
#[derive(Debug, Clone)]
pub struct DdnsUpdater<D: DdnsProvider = HttpDdnsProvider> {
    registry: Arc<ServiceRegistry>,
    ddns_state: Arc<DdnsState>,
    provider: Arc<D>,
}

impl<D: DdnsProvider> DdnsUpdater<D> {
    /// Creates a new [`DdnsUpdater`].
    #[must_use]
    pub fn new(registry: Arc<ServiceRegistry>, ddns_state: Arc<DdnsState>, provider: D) -> Self {
        Self {
            registry,
            ddns_state,
            provider: Arc::new(provider),
        }
    }

    /// Runs a single DDNS check-and-update cycle if DDNS is enabled.
    ///
    /// Returns `Ok(true)` if a record update was sent to the DDNS provider, `Ok(false)` if
    /// skipped (disabled, no healthy backends, or IP unchanged), or `Err` on failure.
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::DdnsFailure`] if resolving the public IP or updating the
    /// DDNS record fails.
    pub async fn sync_once(&self) -> Result<bool, BalancerError> {
        let config = self.ddns_state.config();
        if !config.enabled {
            return Ok(false);
        }

        // Only advertise this balancer instance in DDNS if at least one upstream service has healthy backends
        let registry_snap = self.registry.snapshot().await;
        let has_healthy_service = registry_snap
            .services
            .iter()
            .any(|svc| svc.healthy_backends > 0);

        if !has_healthy_service {
            tracing::warn!(
                instance_id = %config.instance_id,
                hostname = %config.hostname,
                "Skipping DDNS announcement because no upstream services have healthy backends"
            );
            return Ok(false);
        }

        let current_ip = self
            .provider
            .resolve_public_ip(config)
            .await
            .inspect_err(|_| {
                self.ddns_state
                    .failed_updates
                    .fetch_add(1, Ordering::Relaxed);
            })?;

        {
            let guard = self.ddns_state.last_published_ip.read().await;
            if guard.as_deref() == Some(current_ip.as_str()) {
                return Ok(false);
            }
        }

        self.provider
            .update_record(config, &current_ip)
            .await
            .inspect_err(|_| {
                self.ddns_state
                    .failed_updates
                    .fetch_add(1, Ordering::Relaxed);
            })?;

        {
            let mut guard = self.ddns_state.last_published_ip.write().await;
            *guard = Some(current_ip.clone());
        }
        self.ddns_state
            .successful_updates
            .fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            instance_id = %config.instance_id,
            hostname = %config.hostname,
            public_ip = %current_ip,
            "Successfully synchronized public IP with DDNS provider"
        );

        Ok(true)
    }

    /// Runs the periodic DDNS synchronization loop until `shutdown_rx` signals termination.
    pub async fn run_until_shutdown(self, mut shutdown_rx: watch::Receiver<bool>) {
        if !self.ddns_state.config().enabled {
            return;
        }

        let mut ticker = tokio::time::interval(self.ddns_state.config().interval());
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(err) = self.sync_once().await {
                        tracing::warn!(error = %err, "Periodic DDNS synchronization failed");
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        tracing::info!("Shutting down DdnsUpdater");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::config::{
        BackendConfig, BalancerBuilder, BalancerConfig, HealthCheckConfig, ServiceConfig,
        StrategyKind,
    };

    #[derive(Debug, Default)]
    struct MockDdnsProvider {
        current_ip: Mutex<String>,
        published_records: Mutex<Vec<(String, String)>>,
    }

    impl MockDdnsProvider {
        fn with_ip(ip: &str) -> Self {
            Self {
                current_ip: Mutex::new(ip.to_string()),
                published_records: Mutex::new(Vec::new()),
            }
        }

        fn set_ip(&self, ip: &str) {
            *self.current_ip.lock().unwrap() = ip.to_string();
        }
    }

    impl DdnsProvider for MockDdnsProvider {
        async fn resolve_public_ip(&self, _config: &DdnsConfig) -> Result<String, BalancerError> {
            Ok(self.current_ip.lock().unwrap().clone())
        }

        async fn update_record(
            &self,
            config: &DdnsConfig,
            public_ip: &str,
        ) -> Result<(), BalancerError> {
            self.published_records
                .lock()
                .unwrap()
                .push((config.hostname.clone(), public_ip.to_string()));
            Ok(())
        }
    }

    fn setup_ddns_test(enabled: bool) -> (Arc<ServiceRegistry>, Arc<DdnsState>) {
        let ddns = DdnsConfig {
            enabled,
            instance_id: "edge-1".to_string(),
            hostname: "lb.example.ddns.net".to_string(),
            ip_check_url: "https://api.ipify.org".to_string(),
            update_url: "https://ddns.example.com/nic/update".to_string(),
            interval_ms: 1000,
            timeout_ms: 500,
            auth_token: Some("secret-token".to_string()),
        };

        let config = BalancerConfig {
            listen_addr: "127.0.0.1:0".to_string(),
            health_check: HealthCheckConfig::default(),
            ddns: ddns.clone(),
            services: vec![ServiceConfig {
                name: "users".to_string(),
                route_prefix: "/users".to_string(),
                strip_prefix: true,
                strategy: StrategyKind::RoundRobin,
                health_check_path: "/health".to_string(),
                backends: vec![BackendConfig {
                    url: "http://127.0.0.1:3001".to_string(),
                    weight: 1,
                }],
            }],
        };

        let validated = BalancerBuilder::new(config).validate().unwrap();
        let registry = ServiceRegistry::from_validated(&validated);
        let state = Arc::new(DdnsState::new(ddns));
        (registry, state)
    }

    mod sync_once {
        use super::*;

        #[tokio::test]
        async fn publishes_initial_public_ip_and_skips_when_unchanged() {
            let (registry, ddns_state) = setup_ddns_test(true);
            let provider = MockDdnsProvider::with_ip("203.0.113.10");
            let updater = DdnsUpdater::new(registry, Arc::clone(&ddns_state), provider);

            let first_updated = updater.sync_once().await.unwrap();
            let second_updated = updater.sync_once().await.unwrap();
            let snap = ddns_state.snapshot().await;

            assert_eq!(
                (
                    first_updated,
                    second_updated,
                    snap.last_published_ip.as_deref()
                ),
                (true, false, Some("203.0.113.10"))
            );
        }

        #[tokio::test]
        async fn updates_ddns_record_when_public_ip_changes() {
            let (registry, ddns_state) = setup_ddns_test(true);
            let provider = MockDdnsProvider::with_ip("203.0.113.10");
            let updater = DdnsUpdater::new(registry, Arc::clone(&ddns_state), provider);

            updater.sync_once().await.unwrap();
            updater.provider.set_ip("198.51.100.42");
            let updated_on_change = updater.sync_once().await.unwrap();
            let snap = ddns_state.snapshot().await;

            assert_eq!(
                (
                    updated_on_change,
                    snap.successful_updates,
                    snap.last_published_ip.as_deref()
                ),
                (true, 2, Some("198.51.100.42"))
            );
        }

        #[tokio::test]
        async fn skips_ddns_update_when_all_upstream_backends_are_unhealthy() {
            let (registry, ddns_state) = setup_ddns_test(true);
            let pool = registry.get_service("users").await.unwrap();
            for node in pool.backends().await {
                node.set_healthy(false);
            }

            let provider = MockDdnsProvider::with_ip("203.0.113.10");
            let updater = DdnsUpdater::new(registry, Arc::clone(&ddns_state), provider);

            let did_publish = updater.sync_once().await.unwrap();
            assert!(!did_publish);
        }
    }
}
