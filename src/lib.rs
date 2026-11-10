//! `rust-balancer` — Cloud-Native Layer 7 HTTP Load Balancer for $N$ upstream services.
//!
//! This crate provides:
//! - **Compile-time validated configuration** using the Type-State pattern ([`config::BalancerBuilder`]).
//! - **Shared concurrent state** ([`state::ServiceRegistry`], [`state::ServicePool`], [`state::BackendNode`])
//!   powered by lock-free atomic counters on the request path and `RwLock` for dynamic topology changes.
//! - **Abstraction layers** for load-balancing strategies ([`strategy::LoadBalancingStrategy`]),
//!   active health probes ([`health::HealthProbe`]), and HTTP forwarding ([`proxy::UpstreamForwarder`]).
//! - **Active & passive health checking** ([`health::HealthCheckRunner`]) with configurable
//!   recovery and failure thresholds.
//!
//! # Examples
//!
//! ```
//! use rust_balancer::config::{
//!     BackendConfig, BalancerBuilder, BalancerConfig, DdnsConfig, HealthCheckConfig,
//!     ServiceConfig, StrategyKind,
//! };
//! use rust_balancer::state::ServiceRegistry;
//!
//! let config = BalancerConfig {
//!     listen_addr: "127.0.0.1:8080".to_string(),
//!     health_check: HealthCheckConfig::default(),
//!     ddns: DdnsConfig::default(),
//!     services: vec![ServiceConfig {
//!         name: "catalog-service".to_string(),
//!         route_prefix: "/catalog".to_string(),
//!         strip_prefix: true,
//!         strategy: StrategyKind::RoundRobin,
//!         health_check_path: "/health".to_string(),
//!         backends: vec![BackendConfig {
//!             url: "http://127.0.0.1:4001".to_string(),
//!             weight: 1,
//!         }],
//!     }],
//! };
//!
//! let validated = BalancerBuilder::new(config).validate().unwrap();
//! let registry = ServiceRegistry::from_validated(&validated);
//! assert_eq!(registry.health_check_config().unhealthy_threshold, 2);
//! ```

pub mod config;
pub mod ddns;
pub mod error;
pub mod health;
pub mod proxy;
pub mod state;
pub mod strategy;
