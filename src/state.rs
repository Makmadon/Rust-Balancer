//! Shared state management for $N$ upstream cloud services and their backend pools.
//!
//! Follows Chapter 1 and Chapter 9 of the Rust Best Practices handbook:
//! - Uses [`Arc`] and lock-free atomics ([`AtomicBool`], [`AtomicUsize`], [`AtomicU32`],
//!   [`AtomicU64`]) for per-request state updates on [`BackendNode`].
//! - Uses [`tokio::sync::RwLock`] on [`ServiceRegistry`] so concurrent L7 requests share
//!   read access while allowing dynamic runtime registration of new backends or services.

// std
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// external crates
use serde::Serialize;
use tokio::sync::RwLock;

// crate
use crate::config::{
    BackendConfig, BalancerBuilder, HealthCheckConfig, ServiceConfig, StrategyKind, Validated,
};
use crate::error::BalancerError;
use crate::strategy::{build_strategy, LoadBalancingStrategy};

/// Represents a single upstream backend instance with lock-free atomic state counters.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use rust_balancer::state::BackendNode;
///
/// let node = Arc::new(BackendNode::new("http://127.0.0.1:3001", 2));
/// assert!(node.is_healthy());
/// assert_eq!(node.active_connections(), 0);
///
/// {
///     let _guard = node.acquire_connection();
///     assert_eq!(node.active_connections(), 1);
/// }
/// assert_eq!(node.active_connections(), 0);
/// ```
#[derive(Debug)]
pub struct BackendNode {
    url: String,
    weight: u16,
    healthy: AtomicBool,
    active_connections: AtomicUsize,
    total_requests: AtomicU64,
    total_failures: AtomicU64,
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
}

impl BackendNode {
    /// Creates a new initially-healthy [`BackendNode`].
    #[must_use]
    pub fn new(url: &str, weight: u16) -> Self {
        let normalized = url.trim_end_matches('/').to_string();
        Self {
            url: normalized,
            weight: weight.max(1),
            healthy: AtomicBool::new(true),
            active_connections: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
        }
    }

    /// Returns the base URL of this backend node as a borrowed `&str`.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the relative weight assigned to this backend node.
    #[must_use]
    pub const fn weight(&self) -> u16 {
        self.weight
    }

    /// Returns `true` if this backend node is currently marked healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Explicitly sets the health status of this backend node and resets streak counters.
    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
        if healthy {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        } else {
            self.consecutive_successes.store(0, Ordering::Relaxed);
        }
    }

    /// Returns the number of currently in-flight requests to this backend.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Returns the total number of requests routed to this backend since startup.
    #[must_use]
    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Returns the total number of failed requests or probes recorded for this backend.
    #[must_use]
    pub fn total_failures(&self) -> u64 {
        self.total_failures.load(Ordering::Relaxed)
    }

    /// Increments `active_connections` and `total_requests`, returning an RAII
    /// [`ActiveConnectionGuard`] that decrements `active_connections` when dropped.
    #[must_use]
    pub fn acquire_connection(self: &Arc<Self>) -> ActiveConnectionGuard {
        self.active_connections.fetch_add(1, Ordering::AcqRel);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        ActiveConnectionGuard {
            node: Arc::clone(self),
        }
    }

    /// Records a successful probe or upstream response and transitions the node to
    /// healthy if `consecutive_successes >= healthy_threshold`.
    ///
    /// Returns `true` if the node transitioned from unhealthy to healthy.
    pub fn record_success(&self, healthy_threshold: u32) -> bool {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let successes = self.consecutive_successes.fetch_add(1, Ordering::AcqRel) + 1;

        if successes >= healthy_threshold.max(1) {
            let was_healthy = self.healthy.swap(true, Ordering::AcqRel);
            return !was_healthy;
        }
        false
    }

    /// Records a failed probe or upstream connection error and transitions the node to
    /// unhealthy if `consecutive_failures >= unhealthy_threshold`.
    ///
    /// Returns `true` if the node transitioned from healthy to unhealthy.
    pub fn record_failure(&self, unhealthy_threshold: u32) -> bool {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;

        if failures >= unhealthy_threshold.max(1) {
            let was_healthy = self.healthy.swap(false, Ordering::AcqRel);
            return was_healthy;
        }
        false
    }

    /// Produces a serializable snapshot of the node's current metrics.
    #[must_use]
    pub fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            url: self.url.clone(),
            weight: self.weight,
            healthy: self.is_healthy(),
            active_connections: self.active_connections(),
            total_requests: self.total_requests(),
            total_failures: self.total_failures(),
        }
    }
}

/// RAII guard that decrements a [`BackendNode`]'s `active_connections` counter on drop.
#[derive(Debug)]
pub struct ActiveConnectionGuard {
    node: Arc<BackendNode>,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.node.active_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Represents a pool of backend instances serving one logical cloud service.
#[derive(Debug)]
pub struct ServicePool {
    name: String,
    route_prefix: String,
    strip_prefix: bool,
    health_check_path: String,
    strategy: Arc<dyn LoadBalancingStrategy>,
    backends: RwLock<Vec<Arc<BackendNode>>>,
}

impl ServicePool {
    /// Constructs a [`ServicePool`] from a validated [`ServiceConfig`].
    #[must_use]
    pub fn from_config(config: &ServiceConfig) -> Self {
        let nodes = config
            .backends
            .iter()
            .map(|b| Arc::new(BackendNode::new(&b.url, b.weight)))
            .collect();

        Self {
            name: config.name.clone(),
            route_prefix: config.route_prefix.trim_end_matches('/').to_string(),
            strip_prefix: config.strip_prefix,
            health_check_path: config.health_check_path.clone(),
            strategy: build_strategy(config.strategy),
            backends: RwLock::new(nodes),
        }
    }

    /// Returns the logical name of this cloud service.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the normalized route prefix (e.g. `/users`).
    #[must_use]
    pub fn route_prefix(&self) -> &str {
        &self.route_prefix
    }

    /// Returns whether `route_prefix` is stripped when forwarding requests upstream.
    #[must_use]
    pub const fn strip_prefix(&self) -> bool {
        self.strip_prefix
    }

    /// Returns the health check path configured for this service (e.g. `/health`).
    #[must_use]
    pub fn health_check_path(&self) -> &str {
        &self.health_check_path
    }

    /// Returns the [`StrategyKind`] active for this service pool.
    #[must_use]
    pub fn strategy_kind(&self) -> StrategyKind {
        self.strategy.kind()
    }

    /// Selects a healthy backend from the pool using the configured [`LoadBalancingStrategy`].
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::NoHealthyBackends`] if no backend in the pool is currently healthy.
    pub async fn select_backend(&self) -> Result<Arc<BackendNode>, BalancerError> {
        let backends = self.backends.read().await;
        self.strategy
            .select_backend(&backends)
            .cloned()
            .ok_or_else(|| BalancerError::NoHealthyBackends {
                service: self.name.clone(),
            })
    }

    /// Returns a cloned list of [`Arc<BackendNode>`] pointers for background health probing.
    pub async fn backends(&self) -> Vec<Arc<BackendNode>> {
        let guard = self.backends.read().await;
        guard.iter().cloned().collect()
    }

    /// Dynamically registers a new backend node into this service pool at runtime
    /// (or marks it healthy if the same URL is already registered).
    pub async fn add_backend(&self, config: &BackendConfig) {
        let normalized_url = config.url.trim_end_matches('/');
        let mut guard = self.backends.write().await;
        if let Some(existing) = guard.iter().find(|n| n.url() == normalized_url) {
            existing.set_healthy(true);
            return;
        }
        guard.push(Arc::new(BackendNode::new(&config.url, config.weight)));
    }

    /// Computes the upstream target path (and optional query string) for an incoming request.
    #[must_use]
    pub fn rewrite_path(&self, incoming_path: &str, query: Option<&str>) -> String {
        let base_path = if self.strip_prefix && !self.route_prefix.is_empty() {
            let stripped = incoming_path
                .strip_prefix(&self.route_prefix)
                .unwrap_or(incoming_path);
            if stripped.is_empty() {
                "/"
            } else {
                stripped
            }
        } else {
            incoming_path
        };

        if let Some(q) = query.filter(|q| !q.is_empty()) {
            format!("{base_path}?{q}")
        } else {
            base_path.to_string()
        }
    }

    /// Captures a serializable status snapshot of this service pool and all of its backends.
    pub async fn snapshot(&self) -> ServiceSnapshot {
        let guard = self.backends.read().await;
        let backends: Vec<BackendSnapshot> = guard.iter().map(|node| node.snapshot()).collect();
        let healthy_backends = backends.iter().filter(|b| b.healthy).count();

        ServiceSnapshot {
            name: self.name.clone(),
            route_prefix: if self.route_prefix.is_empty() {
                "/".to_string()
            } else {
                self.route_prefix.clone()
            },
            strategy: self.strategy.kind(),
            healthy_backends,
            total_backends: backends.len(),
            backends,
        }
    }
}

/// Shared, thread-safe registry managing all $N$ upstream cloud services.
#[derive(Debug)]
pub struct ServiceRegistry {
    health_check: HealthCheckConfig,
    services: RwLock<HashMap<String, Arc<ServicePool>>>,
}

impl ServiceRegistry {
    /// Builds a [`ServiceRegistry`] from a compile-time validated [`BalancerBuilder<Validated>`].
    #[must_use]
    pub fn from_validated(builder: &BalancerBuilder<Validated>) -> Arc<Self> {
        let config = builder.config();
        let mut map = HashMap::with_capacity(config.services.len());

        for svc in &config.services {
            let pool = Arc::new(ServicePool::from_config(svc));
            map.insert(svc.name.clone(), pool);
        }

        Arc::new(Self {
            health_check: config.health_check,
            services: RwLock::new(map),
        })
    }

    /// Returns the global [`HealthCheckConfig`] associated with this registry.
    #[must_use]
    pub const fn health_check_config(&self) -> HealthCheckConfig {
        self.health_check
    }

    /// Resolves an incoming HTTP request path to the matching [`ServicePool`] using
    /// longest-prefix boundary matching.
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::ServiceNotFound`] if no registered service prefix matches `path`.
    pub async fn resolve_service(&self, path: &str) -> Result<Arc<ServicePool>, BalancerError> {
        let guard = self.services.read().await;

        let matched = guard
            .values()
            .filter(|pool| path_matches_prefix(path, pool.route_prefix()))
            .max_by_key(|pool| pool.route_prefix().len())
            .cloned();

        matched.ok_or_else(|| BalancerError::ServiceNotFound {
            path: path.to_string(),
        })
    }

    /// Looks up a [`ServicePool`] by its exact service name.
    pub async fn get_service(&self, name: &str) -> Option<Arc<ServicePool>> {
        let guard = self.services.read().await;
        guard.get(name).cloned()
    }

    /// Dynamically registers or replaces an upstream [`ServicePool`] at runtime.
    pub async fn register_service(&self, config: &ServiceConfig) {
        let pool = Arc::new(ServicePool::from_config(config));
        let mut guard = self.services.write().await;
        guard.insert(config.name.clone(), pool);
    }

    /// Returns all currently registered [`ServicePool`] instances.
    pub async fn all_services(&self) -> Vec<Arc<ServicePool>> {
        let guard = self.services.read().await;
        guard.values().cloned().collect()
    }

    /// Generates a complete [`RegistrySnapshot`] of all $N$ services for observability endpoints.
    pub async fn snapshot(&self) -> RegistrySnapshot {
        let pools = self.all_services().await;
        let mut services = Vec::with_capacity(pools.len());
        for pool in pools {
            services.push(pool.snapshot().await);
        }
        services.sort_by(|a, b| a.name.cmp(&b.name));

        RegistrySnapshot {
            total_services: services.len(),
            services,
        }
    }
}

/// Checks whether `path` matches `prefix` on a segment boundary (e.g. `/users` matches
/// `/users` and `/users/42`, but not `/users_admin`).
fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return path.starts_with('/');
    }
    let Some(remainder) = path.strip_prefix(prefix) else {
        return false;
    };
    remainder.is_empty() || remainder.starts_with('/')
}

/// Serializable status snapshot of the entire [`ServiceRegistry`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RegistrySnapshot {
    /// Total number of registered upstream cloud services.
    pub total_services: usize,
    /// Per-service snapshots sorted alphabetically by service name.
    pub services: Vec<ServiceSnapshot>,
}

/// Serializable status snapshot of a single [`ServicePool`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ServiceSnapshot {
    /// Logical service name.
    pub name: String,
    /// Configured URL route prefix.
    pub route_prefix: String,
    /// Active load balancing algorithm.
    pub strategy: StrategyKind,
    /// Number of currently healthy backend nodes.
    pub healthy_backends: usize,
    /// Total number of configured backend nodes.
    pub total_backends: usize,
    /// Individual backend metrics.
    pub backends: Vec<BackendSnapshot>,
}

/// Serializable metrics snapshot for a single [`BackendNode`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BackendSnapshot {
    /// Base URL of the backend.
    pub url: String,
    /// Configured weight.
    pub weight: u16,
    /// Current health status.
    pub healthy: bool,
    /// Number of active in-flight connections.
    pub active_connections: usize,
    /// Total requests routed to this node.
    pub total_requests: u64,
    /// Total failures observed for this node.
    pub total_failures: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BalancerConfig;

    fn sample_registry() -> Arc<ServiceRegistry> {
        let config = BalancerConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            health_check: HealthCheckConfig::default(),
            ddns: crate::config::DdnsConfig::default(),
            services: vec![
                ServiceConfig {
                    name: "users".to_string(),
                    route_prefix: "/users".to_string(),
                    strip_prefix: true,
                    strategy: StrategyKind::RoundRobin,
                    health_check_path: "/health".to_string(),
                    backends: vec![BackendConfig {
                        url: "http://127.0.0.1:3001".to_string(),
                        weight: 1,
                    }],
                },
                ServiceConfig {
                    name: "users-VIP".to_string(),
                    route_prefix: "/users/vip".to_string(),
                    strip_prefix: false,
                    strategy: StrategyKind::LeastConnections,
                    health_check_path: "/health".to_string(),
                    backends: vec![BackendConfig {
                        url: "http://127.0.0.1:3009".to_string(),
                        weight: 1,
                    }],
                },
            ],
        };
        let validated = BalancerBuilder::new(config).validate().unwrap();
        ServiceRegistry::from_validated(&validated)
    }

    mod health_transitions {
        use super::*;

        #[test]
        fn marks_node_unhealthy_only_after_reaching_unhealthy_threshold() {
            let node = BackendNode::new("http://127.0.0.1:3001", 1);

            let transitioned_first = node.record_failure(2);
            let transitioned_second = node.record_failure(2);

            assert_eq!(
                (transitioned_first, transitioned_second, node.is_healthy()),
                (false, true, false)
            );
        }

        #[test]
        fn recovers_node_to_healthy_after_reaching_healthy_threshold() {
            let node = BackendNode::new("http://127.0.0.1:3001", 1);
            node.set_healthy(false);

            let first_ok = node.record_success(2);
            let second_ok = node.record_success(2);

            assert_eq!(
                (first_ok, second_ok, node.is_healthy()),
                (false, true, true)
            );
        }
    }

    mod connection_guard {
        use super::*;

        #[test]
        fn decrements_active_connections_when_guard_is_dropped() {
            let node = Arc::new(BackendNode::new("http://127.0.0.1:3001", 1));
            {
                let _g1 = node.acquire_connection();
                let _g2 = node.acquire_connection();
                assert_eq!(node.active_connections(), 2);
            }
            assert_eq!(node.active_connections(), 0);
        }
    }

    mod route_resolution {
        use super::*;

        #[tokio::test]
        async fn resolves_longest_matching_route_prefix() {
            let registry = sample_registry();

            let matched = registry
                .resolve_service("/users/vip/profile")
                .await
                .unwrap();
            assert_eq!(matched.name(), "users-VIP");
        }

        #[tokio::test]
        async fn does_not_match_partial_path_segment_prefix() {
            let registry = sample_registry();

            let result = registry.resolve_service("/users_other/123").await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn strips_prefix_and_preserves_query_string_when_configured() {
            let registry = sample_registry();
            let users_pool = registry.resolve_service("/users/42").await.unwrap();

            let rewritten = users_pool.rewrite_path("/users/42", Some("active=true"));
            assert_eq!(rewritten, "/42?active=true");
        }
    }
}
