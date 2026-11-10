//! Layer 7 HTTP reverse proxy router and upstream forwarding abstraction.
//!
//! Follows Chapter 1, Chapter 4, and Chapter 6 of the Rust Best Practices handbook:
//! - Defines [`UpstreamForwarder`] as a trait abstraction with static dispatch (`<F: UpstreamForwarder>`)
//!   inside [`BalancerAppState<F>`], enabling zero-cost abstraction in production and fast
//!   in-memory unit testing without sockets.
//! - Propagates typed [`BalancerError`] values via `?` with zero `.unwrap()` calls in
//!   production code.

// std
use std::future::Future;
use std::sync::Arc;

// external crates
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde::Serialize;

// crate
use crate::config::{BackendConfig, DdnsConfig};
use crate::ddns::{DdnsSnapshot, DdnsState};
use crate::error::BalancerError;
use crate::state::{RegistrySnapshot, ServiceRegistry, ServiceSnapshot};

/// Maximum request/response body payload buffered per L7 proxy call (16 MiB).
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hop-by-hop HTTP headers that must not be forwarded by an L7 proxy (RFC 7230 §6.1).
const HOP_BY_HOP_HEADERS: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

/// Simplified representation of an outbound HTTP request sent to an upstream backend node.
#[derive(Debug, Clone)]
pub struct OutboundRequest {
    /// HTTP method (`GET`, `POST`, etc.).
    pub method: Method,
    /// Full target URL on the selected backend node.
    pub url: String,
    /// Filtered HTTP headers including `X-Forwarded-*` metadata.
    pub headers: HeaderMap,
    /// Raw request body payload.
    pub body: Bytes,
}

/// Simplified representation of an HTTP response received from an upstream backend node.
#[derive(Debug, Clone)]
pub struct UpstreamResponse {
    /// HTTP status code returned by the backend.
    pub status: StatusCode,
    /// Response headers returned by the backend.
    pub headers: HeaderMap,
    /// Raw response body payload.
    pub body: Bytes,
}

/// Abstraction layer for dispatching an [`OutboundRequest`] to a selected upstream backend.
pub trait UpstreamForwarder: Send + Sync + 'static {
    /// Sends `request` to the target upstream backend and returns its [`UpstreamResponse`].
    fn forward(
        &self,
        request: OutboundRequest,
    ) -> impl Future<Output = Result<UpstreamResponse, BalancerError>> + Send;
}

/// Production HTTP forwarder backed by [`reqwest::Client`] with connection pooling.
#[derive(Debug, Clone)]
pub struct HttpUpstreamForwarder {
    client: reqwest::Client,
}

impl Default for HttpUpstreamForwarder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpUpstreamForwarder {
    /// Creates a new [`HttpUpstreamForwarder`] with default connection pooling.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Creates an [`HttpUpstreamForwarder`] from an existing [`reqwest::Client`].
    #[must_use]
    pub const fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl UpstreamForwarder for HttpUpstreamForwarder {
    async fn forward(&self, request: OutboundRequest) -> Result<UpstreamResponse, BalancerError> {
        let target_url = request.url.clone();
        let mut builder = self.client.request(request.method, &request.url);

        for (name, value) in &request.headers {
            if !is_hop_by_hop_header(name.as_str()) && name != axum::http::header::HOST {
                builder = builder.header(name, value);
            }
        }

        let response = builder.body(request.body).send().await.map_err(|err| {
            BalancerError::UpstreamFailure {
                backend_url: target_url.clone(),
                reason: err.to_string(),
            }
        })?;

        let status = response.status();
        let mut headers = HeaderMap::new();
        for (name, value) in response.headers() {
            if !is_hop_by_hop_header(name.as_str()) {
                headers.insert(name.clone(), value.clone());
            }
        }

        let body = response
            .bytes()
            .await
            .map_err(|err| BalancerError::UpstreamFailure {
                backend_url: target_url,
                reason: err.to_string(),
            })?;

        Ok(UpstreamResponse {
            status,
            headers,
            body,
        })
    }
}

/// Shared application state injected into the Axum L7 router.
#[derive(Debug)]
pub struct BalancerAppState<F: UpstreamForwarder = HttpUpstreamForwarder> {
    registry: Arc<ServiceRegistry>,
    ddns_state: Arc<DdnsState>,
    forwarder: Arc<F>,
}

impl<F: UpstreamForwarder> Clone for BalancerAppState<F> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            ddns_state: Arc::clone(&self.ddns_state),
            forwarder: Arc::clone(&self.forwarder),
        }
    }
}

impl<F: UpstreamForwarder> BalancerAppState<F> {
    /// Creates a new [`BalancerAppState`] combining the shared [`ServiceRegistry`] and [`UpstreamForwarder`].
    #[must_use]
    pub fn new(registry: Arc<ServiceRegistry>, forwarder: F) -> Self {
        Self {
            registry,
            ddns_state: Arc::new(DdnsState::new(DdnsConfig::default())),
            forwarder: Arc::new(forwarder),
        }
    }

    /// Creates a new [`BalancerAppState`] with an explicit shared [`DdnsState`].
    #[must_use]
    pub fn with_ddns(
        registry: Arc<ServiceRegistry>,
        ddns_state: Arc<DdnsState>,
        forwarder: F,
    ) -> Self {
        Self {
            registry,
            ddns_state,
            forwarder: Arc::new(forwarder),
        }
    }

    /// Returns a reference to the underlying [`ServiceRegistry`].
    #[must_use]
    pub fn registry(&self) -> &Arc<ServiceRegistry> {
        &self.registry
    }

    /// Returns a reference to the underlying [`DdnsState`].
    #[must_use]
    pub fn ddns_state(&self) -> &Arc<DdnsState> {
        &self.ddns_state
    }
}

/// Health response payload for the load balancer's own `/_balancer/health` endpoint.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct SelfHealthPayload {
    /// Self-health status (`"ok"`).
    pub status: &'static str,
    /// Number of registered upstream services.
    pub registered_services: usize,
}

/// Builds the Axum [`Router`] for the L7 load balancer.
pub fn build_router<F: UpstreamForwarder>(state: BalancerAppState<F>) -> Router {
    Router::new()
        .route("/_balancer/health", get(self_health_handler::<F>))
        .route("/_balancer/status", get(status_snapshot_handler::<F>))
        .route("/_balancer/ddns", get(ddns_snapshot_handler::<F>))
        .route(
            "/_balancer/services/{service}/backends",
            post(register_backend_handler::<F>),
        )
        .fallback(proxy_request_handler::<F>)
        .with_state(state)
}

async fn self_health_handler<F: UpstreamForwarder>(
    State(state): State<BalancerAppState<F>>,
) -> Json<SelfHealthPayload> {
    let services = state.registry.all_services().await;
    Json(SelfHealthPayload {
        status: "ok",
        registered_services: services.len(),
    })
}

async fn status_snapshot_handler<F: UpstreamForwarder>(
    State(state): State<BalancerAppState<F>>,
) -> Json<RegistrySnapshot> {
    Json(state.registry.snapshot().await)
}

async fn ddns_snapshot_handler<F: UpstreamForwarder>(
    State(state): State<BalancerAppState<F>>,
) -> Json<DdnsSnapshot> {
    Json(state.ddns_state.snapshot().await)
}

async fn register_backend_handler<F: UpstreamForwarder>(
    State(state): State<BalancerAppState<F>>,
    Path(service_name): Path<String>,
    Json(backend): Json<BackendConfig>,
) -> Result<(StatusCode, Json<ServiceSnapshot>), BalancerError> {
    if backend.weight == 0
        || !(backend.url.starts_with("http://") || backend.url.starts_with("https://"))
    {
        return Err(BalancerError::InvalidConfig(
            "backend URL must start with http:// or https:// and weight must be >= 1".to_string(),
        ));
    }

    let pool = state
        .registry
        .get_service(&service_name)
        .await
        .ok_or_else(|| BalancerError::ServiceNotFound {
            path: format!("/_balancer/services/{service_name}/backends"),
        })?;

    pool.add_backend(&backend).await;
    let snapshot = pool.snapshot().await;
    Ok((StatusCode::CREATED, Json(snapshot)))
}

async fn proxy_request_handler<F: UpstreamForwarder>(
    State(state): State<BalancerAppState<F>>,
    request: Request,
) -> Result<Response, BalancerError> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();
    let query = uri.query();

    let service_pool = state.registry.resolve_service(path).await?;
    let backend_node = service_pool.select_backend().await?;
    let _conn_guard = backend_node.acquire_connection();

    let target_relative = service_pool.rewrite_path(path, query);
    let target_url = format!("{}{target_relative}", backend_node.url());

    let mut outbound_headers = request.headers().clone();
    inject_forwarding_headers(&mut outbound_headers, service_pool.name());

    let body_bytes = request
        .into_body()
        .collect()
        .await
        .map_err(|err| BalancerError::UpstreamFailure {
            backend_url: backend_node.url().to_string(),
            reason: format!("failed to read client request body: {err}"),
        })?
        .to_bytes();

    if body_bytes.len() > MAX_BODY_BYTES {
        return Err(BalancerError::UpstreamFailure {
            backend_url: backend_node.url().to_string(),
            reason: "request body exceeds maximum allowed size".to_string(),
        });
    }

    let outbound = OutboundRequest {
        method,
        url: target_url,
        headers: outbound_headers,
        body: body_bytes,
    };

    let hc_config = state.registry.health_check_config();
    let upstream_res = state.forwarder.forward(outbound).await.inspect_err(|err| {
        tracing::warn!(
            service = service_pool.name(),
            backend = backend_node.url(),
            error = %err,
            "Passive health check recorded upstream transport failure"
        );
        backend_node.record_failure(hc_config.unhealthy_threshold);
    })?;

    if upstream_res.status.is_server_error() {
        backend_node.record_failure(hc_config.unhealthy_threshold);
    } else {
        backend_node.record_success(hc_config.healthy_threshold);
    }

    let mut response = Response::builder()
        .status(upstream_res.status)
        .body(Body::from(upstream_res.body))
        .map_err(|err| BalancerError::UpstreamFailure {
            backend_url: backend_node.url().to_string(),
            reason: err.to_string(),
        })?;

    let resp_headers = response.headers_mut();
    for (name, value) in &upstream_res.headers {
        resp_headers.insert(name.clone(), value.clone());
    }
    if let Ok(val) = HeaderValue::from_str(backend_node.url()) {
        resp_headers.insert(HeaderName::from_static("x-balancer-backend"), val);
    }
    if let Ok(val) = HeaderValue::from_str(service_pool.name()) {
        resp_headers.insert(HeaderName::from_static("x-balancer-service"), val);
    }

    Ok(response.into_response())
}

fn inject_forwarding_headers(headers: &mut HeaderMap, service_name: &str) {
    headers
        .entry(HeaderName::from_static("x-forwarded-proto"))
        .or_insert_with(|| HeaderValue::from_static("http"));
    if let Ok(val) = HeaderValue::from_str(service_name) {
        headers.insert(HeaderName::from_static("x-forwarded-service"), val);
    }
}

fn is_hop_by_hop_header(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BackendConfig, BalancerBuilder, BalancerConfig, HealthCheckConfig, ServiceConfig,
        StrategyKind,
    };
    use tower::ServiceExt;

    #[derive(Debug, Default)]
    struct EchoForwarder;

    impl UpstreamForwarder for EchoForwarder {
        async fn forward(
            &self,
            request: OutboundRequest,
        ) -> Result<UpstreamResponse, BalancerError> {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static("x-echo-url"),
                HeaderValue::from_str(&request.url).unwrap(),
            );
            Ok(UpstreamResponse {
                status: StatusCode::OK,
                headers,
                body: request.body,
            })
        }
    }

    fn setup_test_router() -> Router {
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
                backends: vec![
                    BackendConfig {
                        url: "http://127.0.0.1:3001".to_string(),
                        weight: 1,
                    },
                    BackendConfig {
                        url: "http://127.0.0.1:3002".to_string(),
                        weight: 1,
                    },
                ],
            }],
        };
        let validated = BalancerBuilder::new(config).validate().unwrap();
        let registry = ServiceRegistry::from_validated(&validated);
        build_router(BalancerAppState::new(registry, EchoForwarder))
    }

    mod l7_routing {
        use super::*;

        #[tokio::test]
        async fn forwards_request_and_adds_balancer_metadata_headers() {
            let router = setup_test_router();
            let req = Request::builder()
                .method(Method::GET)
                .uri("/users/profile?lang=es")
                .body(Body::empty())
                .unwrap();

            let resp = router.oneshot(req).await.unwrap();

            assert_eq!(
                resp.headers()
                    .get("x-echo-url")
                    .and_then(|v| v.to_str().ok()),
                Some("http://127.0.0.1:3001/profile?lang=es")
            );
        }

        #[tokio::test]
        async fn returns_404_when_path_does_not_match_any_service() {
            let router = setup_test_router();
            let req = Request::builder()
                .method(Method::GET)
                .uri("/unknown/resource")
                .body(Body::empty())
                .unwrap();

            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn responds_200_ok_on_self_health_endpoint() {
            let router = setup_test_router();
            let req = Request::builder()
                .method(Method::GET)
                .uri("/_balancer/health")
                .body(Body::empty())
                .unwrap();

            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }
}
