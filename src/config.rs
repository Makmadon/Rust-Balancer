//! Configuration models and Type-State validation builder for the L7 load balancer.
//!
//! This module uses the **Type-State Pattern** (Chapter 7 of Rust Best Practices) via
//! [`BalancerBuilder<Unvalidated>`] and [`BalancerBuilder<Validated>`] so that an
//! unvalidated configuration cannot be used to initialize the [`crate::state::ServiceRegistry`]
//! at compile time.

// std
use std::collections::HashSet;
use std::marker::PhantomData;
use std::time::Duration;

// external crates
use serde::{Deserialize, Serialize};

// crate
use crate::error::BalancerError;

/// Load balancing algorithm selection for a service pool.
///
/// Implements [`Copy`] because it is a small tag-only enum (Chapter 1, §1.2).
///
/// # Examples
///
/// ```
/// use rust_balancer::config::StrategyKind;
///
/// let strategy = StrategyKind::LeastConnections;
/// assert_eq!(strategy, StrategyKind::LeastConnections);
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    /// Distributes requests sequentially across all healthy backends.
    #[default]
    RoundRobin,
    /// Routes each new request to the healthy backend with the fewest active requests.
    LeastConnections,
    /// Distributes requests proportionally to each backend's configured weight.
    WeightedRoundRobin,
}

/// Active health check parameters shared across or customized per service.
///
/// Implements [`Copy`] because all fields are primitive integers (16 bytes total).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HealthCheckConfig {
    /// Interval between active health check sweeps in milliseconds.
    pub interval_ms: u64,
    /// Request timeout for a single health probe in milliseconds.
    pub timeout_ms: u64,
    /// Number of consecutive probe failures before marking a node unhealthy.
    pub unhealthy_threshold: u32,
    /// Number of consecutive probe successes before marking an unhealthy node healthy again.
    pub healthy_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5_000,
            timeout_ms: 2_000,
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        }
    }
}

impl HealthCheckConfig {
    /// Returns the probe interval as a [`Duration`].
    #[must_use]
    pub const fn interval(self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// Returns the probe request timeout as a [`Duration`].
    #[must_use]
    pub const fn timeout(self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// Configuration for a single upstream backend node within a service pool.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BackendConfig {
    /// Base HTTP/HTTPS URL of the backend instance (e.g. `http://10.0.1.12:3000`).
    pub url: String,
    /// Relative weight used by [`StrategyKind::WeightedRoundRobin`] (must be >= 1).
    #[serde(default = "default_weight")]
    pub weight: u16,
}

const fn default_weight() -> u16 {
    1
}

/// Configuration for an upstream cloud service pool routed by the L7 balancer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ServiceConfig {
    /// Unique logical name of the cloud service (e.g. `users-service`).
    pub name: String,
    /// URL path prefix matched by the L7 router (e.g. `/users`).
    pub route_prefix: String,
    /// Whether to strip `route_prefix` before forwarding the path to the backend.
    #[serde(default = "default_strip_prefix")]
    pub strip_prefix: bool,
    /// Load balancing strategy for this service pool.
    #[serde(default)]
    pub strategy: StrategyKind,
    /// Relative path probed by the active health checker (e.g. `/health`).
    #[serde(default = "default_health_path")]
    pub health_check_path: String,
    /// Upstream backend instances belonging to this service.
    pub backends: Vec<BackendConfig>,
}

const fn default_strip_prefix() -> bool {
    true
}

fn default_health_path() -> String {
    "/health".to_string()
}

/// Dynamic DNS (DDNS) client configuration for multi-instance edge coordination.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DdnsConfig {
    /// Whether periodic DDNS synchronization is enabled for this balancer instance.
    #[serde(default)]
    pub enabled: bool,
    /// Unique identifier for this balancer node (e.g. `edge-node-1`).
    #[serde(default = "default_instance_id")]
    pub instance_id: String,
    /// DNS hostname to keep updated with this instance's public IP (e.g. `lb.example.ddns.net`).
    #[serde(default)]
    pub hostname: String,
    /// Endpoint queried to discover this instance's current public IP address.
    #[serde(default = "default_ip_check_url")]
    pub ip_check_url: String,
    /// DDNS provider HTTP update endpoint (e.g. `https://ddns.example.com/nic/update`).
    #[serde(default)]
    pub update_url: String,
    /// Interval between DDNS IP checks and record refreshes in milliseconds.
    #[serde(default = "default_ddns_interval_ms")]
    pub interval_ms: u64,
    /// HTTP timeout for DDNS calls in milliseconds.
    #[serde(default = "default_ddns_timeout_ms")]
    pub timeout_ms: u64,
    /// Optional bearer/query authentication token for the DDNS provider.
    #[serde(default)]
    pub auth_token: Option<String>,
}

fn default_instance_id() -> String {
    "balancer-node-1".to_string()
}

fn default_ip_check_url() -> String {
    "https://api.ipify.org".to_string()
}

const fn default_ddns_interval_ms() -> u64 {
    60_000
}

const fn default_ddns_timeout_ms() -> u64 {
    5_000
}

impl Default for DdnsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            instance_id: default_instance_id(),
            hostname: String::new(),
            ip_check_url: default_ip_check_url(),
            update_url: String::new(),
            interval_ms: default_ddns_interval_ms(),
            timeout_ms: default_ddns_timeout_ms(),
            auth_token: None,
        }
    }
}

impl DdnsConfig {
    /// Returns the synchronization interval as a [`Duration`].
    #[must_use]
    pub const fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// Returns the HTTP request timeout as a [`Duration`].
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// Top-level configuration for the L7 Load Balancer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BalancerConfig {
    /// Socket address where the load balancer listens (e.g. `0.0.0.0:8080`).
    pub listen_addr: String,
    /// Global active health check settings.
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// Optional Dynamic DNS (DDNS) synchronization settings for multi-instance coordination.
    #[serde(default)]
    pub ddns: DdnsConfig,
    /// List of $N$ upstream cloud services managed by the balancer.
    pub services: Vec<ServiceConfig>,
}

impl BalancerConfig {
    /// Parses a [`BalancerConfig`] from a TOML string without validating invariants yet.
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::InvalidConfig`] if the TOML syntax or schema is invalid.
    pub fn from_toml_str(raw_toml: &str) -> Result<Self, BalancerError> {
        toml::from_str(raw_toml).map_err(|err| BalancerError::InvalidConfig(err.to_string()))
    }
}

/// Marker type representing an unvalidated [`BalancerBuilder`] state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Unvalidated;

/// Marker type representing a compile-time validated [`BalancerBuilder`] state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Validated;

/// Type-State builder that enforces configuration validation before a [`BalancerConfig`]
/// can be consumed by the runtime.
///
/// # Examples
///
/// ```
/// use rust_balancer::config::{
///     BackendConfig, BalancerBuilder, BalancerConfig, DdnsConfig, HealthCheckConfig,
///     ServiceConfig, StrategyKind,
/// };
///
/// let raw = BalancerConfig {
///     listen_addr: "127.0.0.1:8080".to_string(),
///     health_check: HealthCheckConfig::default(),
///     ddns: DdnsConfig::default(),
///     services: vec![ServiceConfig {
///         name: "users".to_string(),
///         route_prefix: "/users".to_string(),
///         strip_prefix: true,
///         strategy: StrategyKind::RoundRobin,
///         health_check_path: "/health".to_string(),
///         backends: vec![BackendConfig {
///             url: "http://127.0.0.1:3001".to_string(),
///             weight: 1,
///         }],
///     }],
/// };
///
/// let validated = BalancerBuilder::new(raw).validate().unwrap();
/// assert_eq!(validated.config().services.len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct BalancerBuilder<State = Unvalidated> {
    config: BalancerConfig,
    _state: PhantomData<State>,
}

impl BalancerBuilder<Unvalidated> {
    /// Wraps an unvalidated [`BalancerConfig`] in the initial [`Unvalidated`] state.
    #[must_use]
    pub const fn new(config: BalancerConfig) -> Self {
        Self {
            config,
            _state: PhantomData,
        }
    }

    /// Parses a TOML string and creates an [`Unvalidated`] builder.
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::InvalidConfig`] if parsing fails.
    pub fn from_toml(raw_toml: &str) -> Result<Self, BalancerError> {
        let config = BalancerConfig::from_toml_str(raw_toml)?;
        Ok(Self::new(config))
    }

    /// Validates all configuration invariants and transitions the builder to [`Validated`].
    ///
    /// # Errors
    ///
    /// Returns [`BalancerError::InvalidConfig`] if:
    /// - `listen_addr` is empty.
    /// - `health_check` intervals or thresholds are zero.
    /// - No services are configured, or service names / route prefixes are duplicated.
    /// - A `route_prefix` or `health_check_path` does not start with `/`.
    /// - Any service has zero backends, an invalid HTTP/HTTPS URL, or a backend weight of `0`.
    pub fn validate(self) -> Result<BalancerBuilder<Validated>, BalancerError> {
        validate_listen_addr(&self.config.listen_addr)?;
        validate_health_check(self.config.health_check)?;
        validate_ddns(&self.config.ddns)?;
        validate_services(&self.config.services)?;

        Ok(BalancerBuilder {
            config: self.config,
            _state: PhantomData,
        })
    }
}

impl BalancerBuilder<Validated> {
    /// Returns a shared reference to the validated [`BalancerConfig`].
    #[must_use]
    pub const fn config(&self) -> &BalancerConfig {
        &self.config
    }

    /// Consumes the validated builder and returns the inner [`BalancerConfig`].
    #[must_use]
    pub fn into_inner(self) -> BalancerConfig {
        self.config
    }
}

fn validate_listen_addr(addr: &str) -> Result<(), BalancerError> {
    if addr.trim().is_empty() {
        return Err(BalancerError::InvalidConfig(
            "listen_addr must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_health_check(hc: HealthCheckConfig) -> Result<(), BalancerError> {
    if hc.interval_ms == 0 || hc.timeout_ms == 0 {
        return Err(BalancerError::InvalidConfig(
            "health_check interval_ms and timeout_ms must be greater than 0".to_string(),
        ));
    }
    if hc.unhealthy_threshold == 0 || hc.healthy_threshold == 0 {
        return Err(BalancerError::InvalidConfig(
            "health_check thresholds must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn validate_ddns(ddns: &DdnsConfig) -> Result<(), BalancerError> {
    if !ddns.enabled {
        return Ok(());
    }
    if ddns.hostname.trim().is_empty() {
        return Err(BalancerError::InvalidConfig(
            "ddns.hostname must not be empty when ddns.enabled is true".to_string(),
        ));
    }
    if !(ddns.update_url.starts_with("http://") || ddns.update_url.starts_with("https://")) {
        return Err(BalancerError::InvalidConfig(
            "ddns.update_url must start with http:// or https:// when ddns.enabled is true"
                .to_string(),
        ));
    }
    if !(ddns.ip_check_url.starts_with("http://") || ddns.ip_check_url.starts_with("https://")) {
        return Err(BalancerError::InvalidConfig(
            "ddns.ip_check_url must start with http:// or https:// when ddns.enabled is true"
                .to_string(),
        ));
    }
    if ddns.interval_ms == 0 || ddns.timeout_ms == 0 {
        return Err(BalancerError::InvalidConfig(
            "ddns interval_ms and timeout_ms must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn validate_services(services: &[ServiceConfig]) -> Result<(), BalancerError> {
    if services.is_empty() {
        return Err(BalancerError::InvalidConfig(
            "at least one upstream service must be configured".to_string(),
        ));
    }

    let mut seen_names = HashSet::with_capacity(services.len());
    let mut seen_prefixes = HashSet::with_capacity(services.len());

    for service in services {
        if service.name.trim().is_empty() {
            return Err(BalancerError::InvalidConfig(
                "service name must not be empty".to_string(),
            ));
        }
        if !seen_names.insert(service.name.as_str()) {
            return Err(BalancerError::InvalidConfig(format!(
                "duplicate service name `{}`",
                service.name
            )));
        }
        if !service.route_prefix.starts_with('/') {
            return Err(BalancerError::InvalidConfig(format!(
                "route_prefix `{}` for service `{}` must start with `/`",
                service.route_prefix, service.name
            )));
        }
        if !seen_prefixes.insert(service.route_prefix.as_str()) {
            return Err(BalancerError::InvalidConfig(format!(
                "duplicate route_prefix `{}`",
                service.route_prefix
            )));
        }
        if !service.health_check_path.starts_with('/') {
            return Err(BalancerError::InvalidConfig(format!(
                "health_check_path `{}` for service `{}` must start with `/`",
                service.health_check_path, service.name
            )));
        }
        if service.backends.is_empty() {
            return Err(BalancerError::InvalidConfig(format!(
                "service `{}` must define at least one backend",
                service.name
            )));
        }

        for backend in &service.backends {
            validate_backend(&service.name, backend)?;
        }
    }

    Ok(())
}

fn validate_backend(service_name: &str, backend: &BackendConfig) -> Result<(), BalancerError> {
    if backend.weight == 0 {
        return Err(BalancerError::InvalidConfig(format!(
            "backend `{}` in service `{service_name}` must have weight >= 1",
            backend.url
        )));
    }
    if !(backend.url.starts_with("http://") || backend.url.starts_with("https://")) {
        return Err(BalancerError::InvalidConfig(format!(
            "backend URL `{}` in service `{service_name}` must start with http:// or https://",
            backend.url
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_valid_config() -> BalancerConfig {
        BalancerConfig {
            listen_addr: "0.0.0.0:8080".to_string(),
            health_check: HealthCheckConfig::default(),
            ddns: DdnsConfig::default(),
            services: vec![ServiceConfig {
                name: "users-service".to_string(),
                route_prefix: "/users".to_string(),
                strip_prefix: true,
                strategy: StrategyKind::RoundRobin,
                health_check_path: "/health".to_string(),
                backends: vec![BackendConfig {
                    url: "http://127.0.0.1:3001".to_string(),
                    weight: 1,
                }],
            }],
        }
    }

    mod validate {
        use super::*;

        #[test]
        fn transitions_to_validated_when_all_invariants_hold() {
            let config = sample_valid_config();
            let result = BalancerBuilder::new(config).validate();
            assert!(result.is_ok(), "Expected valid config, got: {result:?}");
        }

        #[test]
        fn returns_error_when_services_list_is_empty() {
            let mut config = sample_valid_config();
            config.services.clear();

            let err = BalancerBuilder::new(config).validate().unwrap_err();
            assert_eq!(
                err,
                BalancerError::InvalidConfig(
                    "at least one upstream service must be configured".to_string()
                )
            );
        }

        #[test]
        fn returns_error_when_route_prefix_does_not_start_with_slash() {
            let mut config = sample_valid_config();
            config.services[0].route_prefix = "users".to_string();

            let err = BalancerBuilder::new(config).validate().unwrap_err();
            assert_eq!(
                err,
                BalancerError::InvalidConfig(
                    "route_prefix `users` for service `users-service` must start with `/`"
                        .to_string()
                )
            );
        }

        #[test]
        fn returns_error_when_duplicate_service_names_exist() {
            let mut config = sample_valid_config();
            let mut duplicate = config.services[0].clone();
            duplicate.route_prefix = "/users-v2".to_string();
            config.services.push(duplicate);

            let err = BalancerBuilder::new(config).validate().unwrap_err();
            assert_eq!(
                err,
                BalancerError::InvalidConfig("duplicate service name `users-service`".to_string())
            );
        }

        #[test]
        fn returns_error_when_backend_weight_is_zero() {
            let mut config = sample_valid_config();
            config.services[0].backends[0].weight = 0;

            let err = BalancerBuilder::new(config).validate().unwrap_err();
            assert_eq!(
                err,
                BalancerError::InvalidConfig(
                    "backend `http://127.0.0.1:3001` in service `users-service` must have weight >= 1"
                        .to_string()
                )
            );
        }

        #[test]
        fn returns_error_when_backend_url_has_invalid_scheme() {
            let mut config = sample_valid_config();
            config.services[0].backends[0].url = "ftp://127.0.0.1:3001".to_string();

            let err = BalancerBuilder::new(config).validate().unwrap_err();
            assert_eq!(
                err,
                BalancerError::InvalidConfig(
                    "backend URL `ftp://127.0.0.1:3001` in service `users-service` must start with http:// or https://"
                        .to_string()
                )
            );
        }
    }

    mod toml_parsing {
        use super::*;

        #[test]
        fn parses_valid_toml_configuration_with_multiple_services() {
            let raw = r#"
                listen_addr = "0.0.0.0:8080"

                [health_check]
                interval_ms = 3000
                timeout_ms = 1000
                unhealthy_threshold = 3
                healthy_threshold = 2

                [[services]]
                name = "catalog"
                route_prefix = "/catalog"
                strategy = "least_connections"
                backends = [{ url = "http://127.0.0.1:4001", weight = 2 }]
            "#;

            let validated = BalancerBuilder::from_toml(raw)
                .and_then(BalancerBuilder::validate)
                .unwrap();

            assert_eq!(
                validated.config().services[0].strategy,
                StrategyKind::LeastConnections
            );
        }
    }
}
