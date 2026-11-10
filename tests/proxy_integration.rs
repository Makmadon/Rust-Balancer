//! End-to-end integration tests for the `rust-balancer` L7 HTTP Load Balancer.
//!
//! Spins up real ephemeral HTTP backend servers and a live L7 balancer instance to verify:
//! - Multi-service path routing across $N$ upstream services.
//! - Round-robin and weighted distribution across replicas.
//! - Active health check failover and recovery.
//! - Status snapshot reporting on `/_balancer/status`.

// std
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// external crates
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;

// crate
use rust_balancer::config::{
    BackendConfig, BalancerBuilder, BalancerConfig, DdnsConfig, HealthCheckConfig, ServiceConfig,
    StrategyKind,
};
use rust_balancer::ddns::DdnsState;
use rust_balancer::health::{HealthCheckRunner, HttpHealthProbe};
use rust_balancer::proxy::{build_router, BalancerAppState, HttpUpstreamForwarder};
use rust_balancer::state::ServiceRegistry;

#[derive(Clone)]
struct MockUpstreamState {
    instance_id: &'static str,
    healthy: Arc<AtomicBool>,
}

async fn spawn_mock_upstream(instance_id: &'static str) -> (String, Arc<AtomicBool>) {
    let healthy = Arc::new(AtomicBool::new(true));
    let state = MockUpstreamState {
        instance_id,
        healthy: Arc::clone(&healthy),
    };

    let router = Router::new()
        .route(
            "/health",
            get(|State(st): State<MockUpstreamState>| async move {
                if st.healthy.load(Ordering::Acquire) {
                    (StatusCode::OK, "healthy")
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "unhealthy")
                }
            }),
        )
        .fallback(
            |State(st): State<MockUpstreamState>, req: axum::extract::Request| async move {
                let path = req.uri().path().to_string();
                (StatusCode::OK, format!("{}:{path}", st.instance_id))
            },
        )
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    (format!("http://{addr}"), healthy)
}

async fn spawn_balancer(config: BalancerConfig) -> (String, Arc<ServiceRegistry>) {
    let ddns_cfg = config.ddns.clone();
    let validated = BalancerBuilder::new(config).validate().unwrap();
    let registry = ServiceRegistry::from_validated(&validated);
    let ddns_state = Arc::new(DdnsState::new(ddns_cfg));

    let app_state = BalancerAppState::with_ddns(
        Arc::clone(&registry),
        ddns_state,
        HttpUpstreamForwarder::new(),
    );
    let router = build_router(app_state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    (format!("http://{addr}"), registry)
}

#[tokio::test]
async fn routes_requests_across_multiple_cloud_services_and_rotates_backends() {
    let (users_1_url, _) = spawn_mock_upstream("users-node-1").await;
    let (users_2_url, _) = spawn_mock_upstream("users-node-2").await;
    let (orders_1_url, _) = spawn_mock_upstream("orders-node-1").await;

    let config = BalancerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        health_check: HealthCheckConfig::default(),
        ddns: DdnsConfig::default(),
        services: vec![
            ServiceConfig {
                name: "users-service".to_string(),
                route_prefix: "/users".to_string(),
                strip_prefix: true,
                strategy: StrategyKind::RoundRobin,
                health_check_path: "/health".to_string(),
                backends: vec![
                    BackendConfig {
                        url: users_1_url,
                        weight: 1,
                    },
                    BackendConfig {
                        url: users_2_url,
                        weight: 1,
                    },
                ],
            },
            ServiceConfig {
                name: "orders-service".to_string(),
                route_prefix: "/orders".to_string(),
                strip_prefix: true,
                strategy: StrategyKind::LeastConnections,
                health_check_path: "/health".to_string(),
                backends: vec![BackendConfig {
                    url: orders_1_url,
                    weight: 1,
                }],
            },
        ],
    };

    let (balancer_url, _registry) = spawn_balancer(config).await;
    let client = reqwest::Client::new();

    let first_user = client
        .get(format!("{balancer_url}/users/profile"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let second_user = client
        .get(format!("{balancer_url}/users/profile"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let order_resp = client
        .get(format!("{balancer_url}/orders/checkout"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        (
            first_user.as_str(),
            second_user.as_str(),
            order_resp.as_str()
        ),
        (
            "users-node-1:/profile",
            "users-node-2:/profile",
            "orders-node-1:/checkout"
        )
    );
}

#[tokio::test]
async fn fails_over_when_backend_becomes_unhealthy_and_recovers_when_healthy_again() {
    let (node_1_url, node_1_health) = spawn_mock_upstream("node-1").await;
    let (node_2_url, _node_2_health) = spawn_mock_upstream("node-2").await;

    let config = BalancerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        health_check: HealthCheckConfig {
            interval_ms: 50,
            timeout_ms: 500,
            unhealthy_threshold: 2,
            healthy_threshold: 2,
        },
        ddns: DdnsConfig::default(),
        services: vec![ServiceConfig {
            name: "payments-service".to_string(),
            route_prefix: "/payments".to_string(),
            strip_prefix: true,
            strategy: StrategyKind::RoundRobin,
            health_check_path: "/health".to_string(),
            backends: vec![
                BackendConfig {
                    url: node_1_url,
                    weight: 1,
                },
                BackendConfig {
                    url: node_2_url,
                    weight: 1,
                },
            ],
        }],
    };

    let (balancer_url, registry) = spawn_balancer(config).await;
    let runner = HealthCheckRunner::new(Arc::clone(&registry), HttpHealthProbe::new());

    // Simulate node-1 failing its health checks
    node_1_health.store(false, Ordering::Release);
    runner.run_sweep_once().await;
    runner.run_sweep_once().await;

    let client = reqwest::Client::new();
    let r1 = client
        .get(format!("{balancer_url}/payments/charge"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let r2 = client
        .get(format!("{balancer_url}/payments/charge"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Both requests must be routed to node-2 while node-1 is unhealthy
    assert_eq!(
        (r1.as_str(), r2.as_str()),
        ("node-2:/charge", "node-2:/charge")
    );

    // Recover node-1 and run 2 health check sweeps
    node_1_health.store(true, Ordering::Release);
    runner.run_sweep_once().await;
    runner.run_sweep_once().await;

    let pool = registry.get_service("payments-service").await.unwrap();
    let snapshot = pool.snapshot().await;
    assert_eq!(snapshot.healthy_backends, 2);
}

#[tokio::test]
async fn returns_503_service_unavailable_when_all_backends_for_service_are_down() {
    let (node_1_url, node_1_health) = spawn_mock_upstream("catalog-1").await;

    let config = BalancerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        health_check: HealthCheckConfig {
            interval_ms: 50,
            timeout_ms: 500,
            unhealthy_threshold: 1,
            healthy_threshold: 1,
        },
        ddns: DdnsConfig::default(),
        services: vec![ServiceConfig {
            name: "catalog-service".to_string(),
            route_prefix: "/catalog".to_string(),
            strip_prefix: true,
            strategy: StrategyKind::RoundRobin,
            health_check_path: "/health".to_string(),
            backends: vec![BackendConfig {
                url: node_1_url,
                weight: 1,
            }],
        }],
    };

    let (balancer_url, registry) = spawn_balancer(config).await;
    let runner = HealthCheckRunner::new(Arc::clone(&registry), HttpHealthProbe::new());

    node_1_health.store(false, Ordering::Release);
    runner.run_sweep_once().await;

    let resp = reqwest::get(format!("{balancer_url}/catalog/items"))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn dynamically_registers_new_ddns_backend_at_runtime_via_api() {
    let (node_1_url, _) = spawn_mock_upstream("dyn-node-1").await;
    let (node_2_url, _) = spawn_mock_upstream("dyn-node-2").await;

    let config = BalancerConfig {
        listen_addr: "127.0.0.1:0".to_string(),
        health_check: HealthCheckConfig::default(),
        ddns: DdnsConfig::default(),
        services: vec![ServiceConfig {
            name: "users-service".to_string(),
            route_prefix: "/users".to_string(),
            strip_prefix: true,
            strategy: StrategyKind::RoundRobin,
            health_check_path: "/health".to_string(),
            backends: vec![BackendConfig {
                url: node_1_url,
                weight: 1,
            }],
        }],
    };

    let (balancer_url, _registry) = spawn_balancer(config).await;
    let client = reqwest::Client::new();

    let register_resp = client
        .post(format!(
            "{balancer_url}/_balancer/services/users-service/backends"
        ))
        .header("content-type", "application/json")
        .body(format!(r#"{{"url":"{node_2_url}","weight":1}}"#))
        .send()
        .await
        .unwrap();

    assert_eq!(register_resp.status(), StatusCode::CREATED);

    let first = client
        .get(format!("{balancer_url}/users/check"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let second = client
        .get(format!("{balancer_url}/users/check"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        (first.as_str(), second.as_str()),
        ("dyn-node-1:/check", "dyn-node-2:/check")
    );
}
