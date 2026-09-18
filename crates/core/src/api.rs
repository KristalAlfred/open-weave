use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{DesiredHop, PathStatus, ReconcileStatus, StreamEndpoints, ValidationIssue};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum StatusResponse {
    Starting(StartingStatus),
    Running(RunningStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartingStatus {
    pub status: StartingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartingState {
    Starting,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunningStatus {
    pub status: ReconcileStatus,
    pub summary: String,
    pub streams: Vec<StreamStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamStatus {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
    pub status: PathStatus,
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<StreamEndpoints>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamAccepted {
    pub status: AcceptedState,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeAccepted {
    pub status: AcceptedState,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AcceptedState {
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamPlan {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
    pub status: PlanStatus,
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub nodes: Vec<String>,
    pub hops: Vec<DesiredHop>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<StreamEndpoints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Disabled,
    Placed,
    Unplaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
