//! Load balancing strategy abstraction layer and implementations.
//!
//! Follows Chapter 6 of the Rust Best Practices handbook:
//! - Defines [`LoadBalancingStrategy`] as an object-safe trait so each [`crate::state::ServicePool`]
//!   can hold its own configured strategy (`RoundRobin`, `LeastConnections`, `WeightedRoundRobin`)
//!   behind `Arc<dyn LoadBalancingStrategy>`.
//! - Uses lock-free atomic operations on the hot path with zero heap allocations per request.

// std
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// crate
use crate::config::StrategyKind;
use crate::state::BackendNode;

/// Trait abstraction for selecting an upstream backend node from a slice of candidates.
///
/// Implementations must be thread-safe (`Send + Sync`) and avoid blocking or allocating
/// on the request path.
pub trait LoadBalancingStrategy: Send + Sync + Debug {
    /// Returns the [`StrategyKind`] identifier for observability and status reporting.
    fn kind(&self) -> StrategyKind;

    /// Selects a healthy [`BackendNode`] from `backends`, or returns `None` if no
    /// backend in the slice is currently healthy.
    fn select_backend<'a>(&self, backends: &'a [Arc<BackendNode>]) -> Option<&'a Arc<BackendNode>>;
}

/// Round-robin load balancing strategy using a lock-free atomic cursor.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use rust_balancer::state::BackendNode;
/// use rust_balancer::strategy::{LoadBalancingStrategy, RoundRobinStrategy};
///
/// let nodes = vec![
///     Arc::new(BackendNode::new("http://127.0.0.1:3001", 1)),
///     Arc::new(BackendNode::new("http://127.0.0.1:3002", 1)),
/// ];
/// let strategy = RoundRobinStrategy::new();
///
/// let first = strategy.select_backend(&nodes).unwrap();
/// let second = strategy.select_backend(&nodes).unwrap();
/// assert_eq!(first.url(), "http://127.0.0.1:3001");
/// assert_eq!(second.url(), "http://127.0.0.1:3002");
/// ```
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    cursor: AtomicUsize,
}

impl RoundRobinStrategy {
    /// Creates a new [`RoundRobinStrategy`] starting at index `0`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cursor: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancingStrategy for RoundRobinStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::RoundRobin
    }

    fn select_backend<'a>(&self, backends: &'a [Arc<BackendNode>]) -> Option<&'a Arc<BackendNode>> {
        let len = backends.len();
        if len == 0 {
            return None;
        }

        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..len {
            let candidate = &backends[(start + offset) % len];
            if candidate.is_healthy() {
                return Some(candidate);
            }
        }
        None
    }
}

/// Least-connections load balancing strategy.
///
/// Routes each incoming request to the healthy backend node with the lowest number of
/// currently active in-flight connections. Ties are broken using a rotating atomic cursor
/// to prevent herd effects on index `0`.
#[derive(Debug, Default)]
pub struct LeastConnectionsStrategy {
    tie_breaker: AtomicUsize,
}

impl LeastConnectionsStrategy {
    /// Creates a new [`LeastConnectionsStrategy`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tie_breaker: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancingStrategy for LeastConnectionsStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::LeastConnections
    }

    fn select_backend<'a>(&self, backends: &'a [Arc<BackendNode>]) -> Option<&'a Arc<BackendNode>> {
        let len = backends.len();
        if len == 0 {
            return None;
        }

        let start = self.tie_breaker.fetch_add(1, Ordering::Relaxed);
        (0..len)
            .map(|offset| &backends[(start + offset) % len])
            .filter(|node| node.is_healthy())
            .min_by_key(|node| node.active_connections())
    }
}

/// Weighted round-robin load balancing strategy.
///
/// Distributes traffic across healthy nodes proportionally to their configured `weight`.
#[derive(Debug, Default)]
pub struct WeightedRoundRobinStrategy {
    cursor: AtomicUsize,
}

impl WeightedRoundRobinStrategy {
    /// Creates a new [`WeightedRoundRobinStrategy`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cursor: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancingStrategy for WeightedRoundRobinStrategy {
    fn kind(&self) -> StrategyKind {
        StrategyKind::WeightedRoundRobin
    }

    fn select_backend<'a>(&self, backends: &'a [Arc<BackendNode>]) -> Option<&'a Arc<BackendNode>> {
        let total_weight: usize = backends
            .iter()
            .filter(|node| node.is_healthy())
            .map(|node| usize::from(node.weight()))
            .sum();

        if total_weight == 0 {
            return None;
        }

        let ticket = self.cursor.fetch_add(1, Ordering::Relaxed) % total_weight;
        let mut cumulative = 0usize;

        for node in backends.iter().filter(|node| node.is_healthy()) {
            cumulative += usize::from(node.weight());
            if ticket < cumulative {
                return Some(node);
            }
        }

        None
    }
}

/// Factory function that creates the boxed [`LoadBalancingStrategy`] corresponding to `kind`.
#[must_use]
pub fn build_strategy(kind: StrategyKind) -> Arc<dyn LoadBalancingStrategy> {
    match kind {
        StrategyKind::RoundRobin => Arc::new(RoundRobinStrategy::new()),
        StrategyKind::LeastConnections => Arc::new(LeastConnectionsStrategy::new()),
        StrategyKind::WeightedRoundRobin => Arc::new(WeightedRoundRobinStrategy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_nodes() -> Vec<Arc<BackendNode>> {
        vec![
            Arc::new(BackendNode::new("http://127.0.0.1:3001", 1)),
            Arc::new(BackendNode::new("http://127.0.0.1:3002", 1)),
            Arc::new(BackendNode::new("http://127.0.0.1:3003", 1)),
        ]
    }

    mod round_robin {
        use super::*;

        #[test]
        fn cycles_sequentially_across_all_healthy_backends() {
            let nodes = sample_nodes();
            let strategy = RoundRobinStrategy::new();

            let selected: Vec<&str> = (0..4)
                .map(|_| strategy.select_backend(&nodes).unwrap().url())
                .collect();

            assert_eq!(
                selected,
                vec![
                    "http://127.0.0.1:3001",
                    "http://127.0.0.1:3002",
                    "http://127.0.0.1:3003",
                    "http://127.0.0.1:3001",
                ]
            );
        }

        #[test]
        fn skips_unhealthy_backend_nodes() {
            let nodes = sample_nodes();
            nodes[1].set_healthy(false);
            let strategy = RoundRobinStrategy::new();

            let first = strategy.select_backend(&nodes).unwrap().url();
            let second = strategy.select_backend(&nodes).unwrap().url();

            assert_eq!(
                (first, second),
                ("http://127.0.0.1:3001", "http://127.0.0.1:3003")
            );
        }

        #[test]
        fn returns_none_when_all_backends_are_unhealthy() {
            let nodes = sample_nodes();
            for node in &nodes {
                node.set_healthy(false);
            }
            let strategy = RoundRobinStrategy::new();

            assert!(strategy.select_backend(&nodes).is_none());
        }
    }

    mod least_connections {
        use super::*;

        #[test]
        fn selects_healthy_backend_with_fewest_active_connections() {
            let nodes = sample_nodes();
            let _guard_1a = nodes[0].acquire_connection();
            let _guard_1b = nodes[0].acquire_connection();
            let _guard_2a = nodes[1].acquire_connection();
            // nodes[2] has 0 active connections

            let strategy = LeastConnectionsStrategy::new();
            let chosen = strategy.select_backend(&nodes).unwrap();

            assert_eq!(chosen.url(), "http://127.0.0.1:3003");
        }

        #[test]
        fn ignores_unhealthy_node_even_if_it_has_zero_connections() {
            let nodes = sample_nodes();
            let _guard_1 = nodes[0].acquire_connection();
            let _guard_2 = nodes[1].acquire_connection();
            nodes[2].set_healthy(false);

            let strategy = LeastConnectionsStrategy::new();
            let chosen = strategy.select_backend(&nodes).unwrap();

            assert_ne!(chosen.url(), "http://127.0.0.1:3003");
        }
    }

    mod weighted_round_robin {
        use super::*;

        #[test]
        fn distributes_requests_proportionally_to_node_weights() {
            let nodes = vec![
                Arc::new(BackendNode::new("http://127.0.0.1:5001", 3)),
                Arc::new(BackendNode::new("http://127.0.0.1:5002", 1)),
            ];
            let strategy = WeightedRoundRobinStrategy::new();

            let count_primary = (0..8)
                .filter(|_| {
                    strategy.select_backend(&nodes).unwrap().url() == "http://127.0.0.1:5001"
                })
                .count();

            assert_eq!(count_primary, 6);
        }
    }
}
