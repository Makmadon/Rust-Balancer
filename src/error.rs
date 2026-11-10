//! Error types for the `rust-balancer` crate.
//!
//! All fallible operations in this library return [`BalancerError`] rather than panicking,
//! following Chapter 4 of the Rust Best Practices handbook.

// external crates
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Crate-level error enumeration representing all failure modes in the L7 load balancer.
///
/// # Examples
///
/// ```
/// use rust_balancer::error::BalancerError;
///
/// let err = BalancerError::ServiceNotFound {
///     path: "/unknown".to_string(),
/// };
/// assert_eq!(err.status_code(), axum::http::StatusCode::NOT_FOUND);
/// ```
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum BalancerError {
    /// Configuration validation or parsing failed.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// No registered service matched the incoming request path.
    #[error("No upstream service matched request path `{path}`")]
    ServiceNotFound {
        /// The unmatched request path.
        path: String,
    },

    /// The matched service exists, but all of its backend nodes are currently unhealthy.
    #[error("No healthy backends available for service `{service}`")]
    NoHealthyBackends {
        /// The name of the upstream service pool.
        service: String,
    },

    /// Forwarding the request to the selected upstream backend failed.
    #[error("Upstream request to `{backend_url}` failed: {reason}")]
    UpstreamFailure {
        /// The URL of the backend that failed.
        backend_url: String,
        /// Human-readable failure description.
        reason: String,
    },

    /// Health check probe failed to execute.
    #[error("Health probe failed for `{backend_url}`: {reason}")]
    HealthProbeFailure {
        /// The URL of the backend being probed.
        backend_url: String,
        /// Reason for the probe error.
        reason: String,
    },

    /// Dynamic DNS (DDNS) public IP resolution or record update failed.
    #[error("DDNS synchronization failed for hostname `{hostname}`: {reason}")]
    DdnsFailure {
        /// The DDNS hostname being updated.
        hostname: String,
        /// Explanation of the failure.
        reason: String,
    },
}

/// JSON error payload returned to HTTP clients when proxy routing fails.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ErrorResponse {
    /// Short machine-readable error code.
    pub error: &'static str,
    /// Detailed error message.
    pub message: String,
}

impl BalancerError {
    /// Maps the domain error to the corresponding HTTP [`StatusCode`].
    ///
    /// # Examples
    ///
    /// ```
    /// use axum::http::StatusCode;
    /// use rust_balancer::error::BalancerError;
    ///
    /// let err = BalancerError::NoHealthyBackends {
    ///     service: "orders".to_string(),
    /// };
    /// assert_eq!(err.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    /// ```
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidConfig(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceNotFound { .. } => StatusCode::NOT_FOUND,
            Self::NoHealthyBackends { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamFailure { .. }
            | Self::HealthProbeFailure { .. }
            | Self::DdnsFailure { .. } => StatusCode::BAD_GATEWAY,
        }
    }

    /// Returns a static machine-readable error code identifier.
    #[must_use]
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidConfig(_) => "INVALID_CONFIG",
            Self::ServiceNotFound { .. } => "SERVICE_NOT_FOUND",
            Self::NoHealthyBackends { .. } => "NO_HEALTHY_BACKENDS",
            Self::UpstreamFailure { .. } => "UPSTREAM_FAILURE",
            Self::HealthProbeFailure { .. } => "HEALTH_PROBE_FAILURE",
            Self::DdnsFailure { .. } => "DDNS_FAILURE",
        }
    }
}

impl IntoResponse for BalancerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorResponse {
            error: self.error_code(),
            message: self.to_string(),
        };
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod status_code {
        use super::*;

        #[test]
        fn returns_not_found_when_service_is_missing() {
            let err = BalancerError::ServiceNotFound {
                path: "/v1/missing".to_string(),
            };
            assert_eq!(err.status_code(), StatusCode::NOT_FOUND);
        }

        #[test]
        fn returns_service_unavailable_when_no_backends_are_healthy() {
            let err = BalancerError::NoHealthyBackends {
                service: "payments".to_string(),
            };
            assert_eq!(err.status_code(), StatusCode::SERVICE_UNAVAILABLE);
        }

        #[test]
        fn returns_bad_gateway_when_upstream_fails() {
            let err = BalancerError::UpstreamFailure {
                backend_url: "http://127.0.0.1:9001".to_string(),
                reason: "connection refused".to_string(),
            };
            assert_eq!(err.status_code(), StatusCode::BAD_GATEWAY);
        }
    }

    mod display_formatting {
        use super::*;

        #[test]
        fn formats_no_healthy_backends_error_with_service_name() {
            let err = BalancerError::NoHealthyBackends {
                service: "users-service".to_string(),
            };
            assert_eq!(
                err.to_string(),
                "No healthy backends available for service `users-service`"
            );
        }
    }
}
