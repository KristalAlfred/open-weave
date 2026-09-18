use serde::{Deserialize, Serialize};

use crate::ValidationIssue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    InvalidRequest,
    InvalidJson,
    Unauthorized,
    RouteNotFound,
    MethodNotAllowed,
    StreamNotFound,
    NodeNotFound,
    StreamNotReady,
    IncompatibleProtocolVersion,
    PersistenceFailed,
    ControllerUnreachable,
    EncodingFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiError {
    pub code: ApiErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<ValidationIssue>,
}

impl ApiError {
    #[must_use]
    pub fn new(code: ApiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_details(
        code: ApiErrorCode,
        message: impl Into<String>,
        details: Vec<ValidationIssue>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            details,
        }
    }
}

#[cfg(feature = "server")]
impl ApiError {
    pub fn response(self, status: axum::http::StatusCode) -> axum::response::Response {
        use axum::response::IntoResponse;

        (status, axum::Json(self)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn details_are_present_only_when_useful() {
        let simple = serde_json::to_value(ApiError::new(
            ApiErrorCode::StreamNotFound,
            "stream not found",
        ))
        .unwrap();
        assert_eq!(
            simple,
            serde_json::json!({
                "code": "stream_not_found",
                "message": "stream not found"
            })
        );

        let detailed = serde_json::to_value(ApiError::with_details(
            ApiErrorCode::InvalidRequest,
            "stream validation failed",
            vec![ValidationIssue {
                field: "name".to_string(),
                code: "invalid_characters".to_string(),
                message: "stream name is invalid".to_string(),
            }],
        ))
        .unwrap();
        assert_eq!(detailed["details"][0]["field"], "name");
    }
}
