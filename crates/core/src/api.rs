use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    DesiredHop, EndpointAddr, PathStatus, ReconcileStatus, StreamDefinition, StreamEndpoints,
    ValidationIssue,
};

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
    #[schemars(range(min = 1))]
    pub generation: u64,
    #[schemars(range(min = 1))]
    pub observed_generation: Option<u64>,
    pub status: PathStatus,
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub nodes: Vec<String>,
    pub conditions: Vec<StreamCondition>,
    pub ingress: Option<EndpointAddr>,
    pub destinations: Vec<StreamDestinationStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamDestinationStatus {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub status: PathStatus,
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub nodes: Vec<String>,
    pub conditions: Vec<StreamCondition>,
    pub endpoint: Option<EndpointAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamCondition {
    #[serde(rename = "type")]
    pub condition_type: StreamConditionType,
    pub status: StreamConditionStatus,
    pub reason: StreamConditionReason,
    pub detail: String,
    #[schemars(extend("format" = "date-time"))]
    pub last_transition_time: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamConditionType {
    PlacementReady,
    NodesAvailable,
    HopsReady,
    FormatCompatible,
    MediaFlowing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamConditionStatus {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamConditionReason {
    Placed,
    SinglePath,
    Disabled,
    PlacementFailed,
    CleartextNotAllowed,
    NodesAvailable,
    NodeMissing,
    NodeOffline,
    HopsReady,
    HopsPending,
    HopFailed,
    FormatCompatible,
    FormatUnknown,
    FormatMismatch,
    MediaFlowing,
    AwaitingInput,
    MediaDegraded,
    MediaFailed,
    MediaIdle,
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamResource {
    #[schemars(range(min = 1))]
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub owner: Option<String>,
    pub spec: StreamDefinition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSetApply {
    pub streams: Vec<StreamDefinition>,
    #[serde(default)]
    pub prune: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSetResource {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub owner: String,
    pub streams: Vec<StreamResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSetAccepted {
    pub status: AcceptedState,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub owner: String,
    pub changed: bool,
    pub streams: Vec<StreamSetMemberResult>,
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub pruned: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSetMemberResult {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
    #[schemars(range(min = 1))]
    pub generation: u64,
    pub action: StreamSetAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamSetAction {
    Created,
    Updated,
    Unchanged,
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
    #[schemars(range(min = 1))]
    pub generation: u64,
    pub changed: bool,
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
    Forbidden,
    RouteNotFound,
    MethodNotAllowed,
    StreamNotFound,
    StreamSetNotFound,
    StreamOwned,
    OwnershipConflict,
    HopIdConflict,
    NodeNotFound,
    StreamNotReady,
    PreconditionRequired,
    PreconditionFailed,
    IncompatibleProtocolVersion,
    PersistenceFailed,
    ControllerUnreachable,
    EncodingFailed,
    NotLeader,
    ControllerTimeout,
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

    #[test]
    fn stream_condition_uses_stable_wire_names() {
        let condition = StreamCondition {
            condition_type: StreamConditionType::MediaFlowing,
            status: StreamConditionStatus::False,
            reason: StreamConditionReason::AwaitingInput,
            detail: "source has not reported media".to_string(),
            last_transition_time: "2026-09-18T09:30:00Z".to_string(),
        };

        assert_eq!(
            serde_json::to_value(condition).unwrap(),
            serde_json::json!({
                "type": "media_flowing",
                "status": "false",
                "reason": "awaiting_input",
                "detail": "source has not reported media",
                "last_transition_time": "2026-09-18T09:30:00Z"
            })
        );
    }

    #[test]
    fn stream_set_contract_uses_stable_wire_names() {
        let accepted = StreamSetAccepted {
            status: AcceptedState::Accepted,
            owner: "production".to_string(),
            changed: true,
            streams: vec![StreamSetMemberResult {
                name: "camera".to_string(),
                generation: 2,
                action: StreamSetAction::Updated,
            }],
            pruned: vec!["old-camera".to_string()],
        };

        assert_eq!(
            serde_json::to_value(accepted).unwrap(),
            serde_json::json!({
                "status": "accepted",
                "owner": "production",
                "changed": true,
                "streams": [{
                    "name": "camera",
                    "generation": 2,
                    "action": "updated"
                }],
                "pruned": ["old-camera"]
            })
        );

        let apply: StreamSetApply = serde_json::from_value(serde_json::json!({
            "streams": []
        }))
        .unwrap();
        assert!(!apply.prune);
    }
}
