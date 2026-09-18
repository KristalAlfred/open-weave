use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::{Map, Value, json};

use crate::{
    API_PREFIX, ApiError, DesiredHop, EndpointDescriptor, NodeAccepted, NodeDescriptor,
    NodeHeartbeat, NodeRegistration, ObservedState, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED,
    ROUTE_NODE_HEARTBEAT, ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS,
    ROUTE_STREAM, ROUTE_STREAM_ENDPOINTS, ROUTE_STREAMS, StatusResponse, StreamAccepted,
    StreamDefinition, StreamEndpoints,
};

pub struct ContractArtifact {
    pub path: &'static str,
    pub content: String,
}

#[must_use]
pub fn artifacts() -> Vec<ContractArtifact> {
    vec![
        artifact("contracts/openapi/northbound-v4.json", northbound_openapi()),
        artifact("contracts/openapi/southbound-v4.json", southbound_openapi()),
        schema_artifact::<ApiError>("contracts/json-schema/v4/api-error.json"),
        schema_artifact::<StatusResponse>("contracts/json-schema/v4/status-response.json"),
        schema_artifact::<StreamAccepted>("contracts/json-schema/v4/stream-accepted.json"),
        schema_artifact::<StreamDefinition>("contracts/json-schema/v4/stream-definition.json"),
        schema_artifact::<StreamEndpoints>("contracts/json-schema/v4/stream-endpoints.json"),
        schema_artifact::<NodeAccepted>("contracts/json-schema/v4/node-accepted.json"),
        schema_artifact::<NodeHeartbeat>("contracts/json-schema/v4/node-heartbeat.json"),
        schema_artifact::<NodeRegistration>("contracts/json-schema/v4/node-registration.json"),
        schema_artifact::<ObservedState>("contracts/json-schema/v4/observed-state.json"),
        schema_artifact::<Vec<DesiredHop>>("contracts/json-schema/v4/desired-hops.json"),
    ]
}

fn artifact(path: &'static str, value: Value) -> ContractArtifact {
    ContractArtifact {
        path,
        content: format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
    }
}

fn schema_artifact<T: JsonSchema>(path: &'static str) -> ContractArtifact {
    artifact(path, schema::<T>())
}

fn schema<T: JsonSchema>() -> Value {
    let mut settings = SchemaSettings::draft2020_12();
    settings.inline_subschemas = true;
    serde_json::to_value(settings.into_generator().into_root_schema_for::<T>()).unwrap()
}

fn components(entries: &[(&str, Value)]) -> Value {
    let schemas: Map<String, Value> = entries
        .iter()
        .map(|(name, schema)| ((*name).to_string(), schema.clone()))
        .collect();
    json!({
        "schemas": schemas,
        "securitySchemes": {
            "bearerAuth": { "type": "http", "scheme": "bearer" }
        }
    })
}

fn schema_ref(name: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

fn json_content(schema: Value) -> Value {
    json!({ "application/json": { "schema": schema } })
}

fn response(description: &str, schema: Option<Value>) -> Value {
    match schema {
        Some(schema) => json!({ "description": description, "content": json_content(schema) }),
        None => json!({ "description": description }),
    }
}

fn request_body(schema_name: &str) -> Value {
    json!({
        "required": true,
        "content": json_content(schema_ref(schema_name))
    })
}

fn path_parameter(name: &str) -> Value {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "schema": {
            "type": "string",
            "minLength": 1,
            "maxLength": 63,
            "pattern": "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
        }
    })
}

fn error_response(description: &str) -> Value {
    response(description, Some(schema_ref("ApiError")))
}

fn common_document(title: &str, paths: Value, components: Value) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": { "title": title, "version": &API_PREFIX[1..] },
        "paths": paths,
        "components": components,
        "security": [{ "bearerAuth": [] }]
    })
}

#[must_use]
pub fn northbound_openapi() -> Value {
    let error = schema::<ApiError>();
    common_document(
        "open-weave northbound API",
        json!({
            format!("{API_PREFIX}{ROUTE_STREAMS}"): {
                "get": {
                    "operationId": "listStreams",
                    "responses": {
                        "200": response("Desired streams", Some(schema_ref("StreamList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                },
                "post": {
                    "operationId": "applyStream",
                    "requestBody": request_body("StreamDefinition"),
                    "responses": {
                        "202": response("Stream accepted", Some(schema_ref("StreamAccepted"))),
                        "400": error_response("Invalid stream"),
                        "401": error_response("Authentication failed"),
                        "500": error_response("Persistence or encoding failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_STREAM}"): {
                "delete": {
                    "operationId": "deleteStream",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "204": response("Stream deleted", None),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_STREAM_ENDPOINTS}"): {
                "get": {
                    "operationId": "getStreamEndpoints",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "200": response("Placed stream endpoints", Some(schema_ref("StreamEndpoints"))),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "503": error_response("Stream not placed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_STATUS}"): {
                "get": {
                    "operationId": "getStatus",
                    "responses": {
                        "200": response("Control-plane status", Some(schema_ref("StatusResponse"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            }
        }),
        components(&[
            ("ApiError", error),
            ("StatusResponse", schema::<StatusResponse>()),
            ("StreamAccepted", schema::<StreamAccepted>()),
            ("StreamDefinition", schema::<StreamDefinition>()),
            ("StreamEndpoints", schema::<StreamEndpoints>()),
            ("StreamList", schema::<Vec<StreamDefinition>>()),
        ]),
    )
}

#[must_use]
pub fn southbound_openapi() -> Value {
    let error = schema::<ApiError>();
    common_document(
        "open-weave southbound API",
        json!({
            format!("{API_PREFIX}{ROUTE_NODES}"): {
                "get": {
                    "operationId": "listNodes",
                    "responses": {
                        "200": response("Registered nodes", Some(schema_ref("NodeList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_NODE_REGISTER}"): {
                "post": {
                    "operationId": "registerNode",
                    "requestBody": request_body("NodeRegistration"),
                    "responses": {
                        "202": response("Node accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid registration"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Incompatible protocol version"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_NODE_HEARTBEAT}"): {
                "post": {
                    "operationId": "heartbeatNode",
                    "parameters": [path_parameter("node_id")],
                    "requestBody": request_body("NodeHeartbeat"),
                    "responses": {
                        "202": response("Heartbeat accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid heartbeat"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Node not found"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_NODE_DESIRED}"): {
                "get": {
                    "operationId": "getDesiredHops",
                    "parameters": [path_parameter("node_id")],
                    "responses": {
                        "200": response("Desired hops", Some(schema_ref("DesiredHopList"))),
                        "400": error_response("Invalid node id"),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_ENDPOINTS}"): {
                "get": {
                    "operationId": "listEndpoints",
                    "responses": {
                        "200": response("Discovered endpoints", Some(schema_ref("EndpointList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            format!("{API_PREFIX}{ROUTE_STATE}"): {
                "get": {
                    "operationId": "getObservedState",
                    "responses": {
                        "200": response("Observed state", Some(schema_ref("ObservedState"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable")
                    }
                }
            }
        }),
        components(&[
            ("ApiError", error),
            ("DesiredHopList", schema::<Vec<DesiredHop>>()),
            ("EndpointList", schema::<Vec<EndpointDescriptor>>()),
            ("NodeAccepted", schema::<NodeAccepted>()),
            ("NodeHeartbeat", schema::<NodeHeartbeat>()),
            ("NodeList", schema::<Vec<NodeDescriptor>>()),
            ("NodeRegistration", schema::<NodeRegistration>()),
            ("ObservedState", schema::<ObservedState>()),
        ]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::Path;

    #[test]
    fn committed_artifacts_match_generated_contracts() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for artifact in artifacts() {
            let actual = std::fs::read_to_string(root.join(artifact.path)).unwrap_or_default();
            assert_eq!(actual, artifact.content, "{} is stale", artifact.path);
        }
    }

    #[test]
    fn openapi_documents_cover_the_versioned_routes() {
        let north = northbound_openapi();
        let south = southbound_openapi();
        assert_eq!(north["openapi"], "3.1.0");
        assert_eq!(south["openapi"], "3.1.0");
        assert_eq!(north["security"][0]["bearerAuth"], json!([]));
        assert_eq!(south["security"][0]["bearerAuth"], json!([]));

        let north_paths: BTreeMap<_, _> = north["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            north_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/v4/status",
                "/v4/streams",
                "/v4/streams/{name}",
                "/v4/streams/{name}/endpoints"
            ]
        );

        let south_paths: BTreeMap<_, _> = south["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            south_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/v4/endpoints",
                "/v4/nodes",
                "/v4/nodes/register",
                "/v4/nodes/{node_id}/desired",
                "/v4/nodes/{node_id}/heartbeat",
                "/v4/state"
            ]
        );
    }

    #[test]
    fn stream_schema_carries_the_resource_id_rule() {
        let schema = serde_json::to_string(&schema::<StreamDefinition>()).unwrap();
        assert!(schema.contains("maxLength"));
        assert!(schema.contains("^[a-z0-9]"));
    }
}
