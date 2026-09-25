use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::{Map, Value, json};

use crate::{
    ApiError, DesiredHop, EndpointDescriptor, NodeAccepted, NodeDescriptor, NodeHeartbeat,
    NodeRegistration, ObservedState, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED, ROUTE_NODE_HEARTBEAT,
    ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS, ROUTE_STREAM,
    ROUTE_STREAM_ENDPOINTS, ROUTE_STREAM_PLANS, ROUTE_STREAM_SET, ROUTE_STREAM_SETS, ROUTE_STREAMS,
    StatusResponse, StreamAccepted, StreamDefinition, StreamEndpoints, StreamPlan, StreamResource,
    StreamSetAccepted, StreamSetApply, StreamSetResource, webhook,
};

pub struct ContractArtifact {
    pub path: &'static str,
    pub content: String,
}

#[must_use]
pub fn artifacts() -> Vec<ContractArtifact> {
    vec![
        artifact("contracts/openapi/northbound.json", northbound_openapi()),
        artifact("contracts/openapi/southbound.json", southbound_openapi()),
        schema_artifact::<ApiError>("contracts/json-schema/api-error.json"),
        schema_artifact::<StatusResponse>("contracts/json-schema/status-response.json"),
        schema_artifact::<StreamAccepted>("contracts/json-schema/stream-accepted.json"),
        schema_artifact::<StreamDefinition>("contracts/json-schema/stream-definition.json"),
        schema_artifact::<StreamEndpoints>("contracts/json-schema/stream-endpoints.json"),
        schema_artifact::<StreamPlan>("contracts/json-schema/stream-plan.json"),
        schema_artifact::<StreamResource>("contracts/json-schema/stream-resource.json"),
        schema_artifact::<StreamSetAccepted>("contracts/json-schema/stream-set-accepted.json"),
        schema_artifact::<StreamSetApply>("contracts/json-schema/stream-set-apply.json"),
        schema_artifact::<StreamSetResource>("contracts/json-schema/stream-set-resource.json"),
        schema_artifact::<NodeAccepted>("contracts/json-schema/node-accepted.json"),
        schema_artifact::<NodeHeartbeat>("contracts/json-schema/node-heartbeat.json"),
        schema_artifact::<NodeRegistration>("contracts/json-schema/node-registration.json"),
        schema_artifact::<ObservedState>("contracts/json-schema/observed-state.json"),
        schema_artifact::<Vec<DesiredHop>>("contracts/json-schema/desired-hops.json"),
        schema_artifact::<webhook::Event>("contracts/json-schema/webhook-event.json"),
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

fn response_with_etag(description: &str, schema: Value) -> Value {
    json!({
        "description": description,
        "headers": {
            "ETag": {
                "description": "Opaque resource revision for conditional mutations",
                "schema": { "type": "string" }
            }
        },
        "content": json_content(schema)
    })
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

fn header_parameter(name: &str, description: &str, required: bool) -> Value {
    json!({
        "name": name,
        "in": "header",
        "description": description,
        "required": required,
        "schema": { "type": "string" }
    })
}

fn error_response(description: &str) -> Value {
    response(description, Some(schema_ref("ApiError")))
}

fn common_document(title: &str, paths: Value, components: Value) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": { "title": title, "version": env!("CARGO_PKG_VERSION") },
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
            ROUTE_NODES: {
                "get": {
                    "operationId": "listNodes",
                    "responses": {
                        "200": response("Registered nodes", Some(schema_ref("NodeList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAMS: {
                "get": {
                    "operationId": "listStreams",
                    "responses": {
                        "200": response("Desired streams", Some(schema_ref("StreamList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "post": {
                    "operationId": "applyStream",
                    "description": "Exactly one of If-Match or If-None-Match is required",
                    "parameters": [
                        header_parameter(
                            "If-Match",
                            "Current ETag when replacing an existing stream",
                            false,
                        ),
                        header_parameter(
                            "If-None-Match",
                            "Use * when creating a stream that must not already exist",
                            false,
                        )
                    ],
                    "requestBody": request_body("StreamDefinition"),
                    "responses": {
                        "202": response_with_etag("Stream accepted", schema_ref("StreamAccepted")),
                        "400": error_response("Invalid stream"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream is owned by a stream set, or can plan a hop id a stored stream can"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence or encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM: {
                "get": {
                    "operationId": "getStream",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "200": response_with_etag("Desired stream", schema_ref("StreamResource")),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "delete": {
                    "operationId": "deleteStream",
                    "parameters": [
                        path_parameter("name"),
                        header_parameter(
                            "If-Match",
                            "Current ETag of the stream to delete",
                            true,
                        )
                    ],
                    "responses": {
                        "204": response("Stream deleted", None),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream is owned by a stream set"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_ENDPOINTS: {
                "get": {
                    "operationId": "getStreamEndpoints",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "200": response("Placed stream endpoints", Some(schema_ref("StreamEndpoints"))),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "503": error_response("Stream not placed, or no controller holds the lease"),
                        "502": error_response("Controller unavailable")
                    }
                }
            },
            ROUTE_STREAM_SETS: {
                "get": {
                    "operationId": "listStreamSets",
                    "responses": {
                        "200": response("Stream ownership sets", Some(schema_ref("StreamSetList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_SET: {
                "get": {
                    "operationId": "getStreamSet",
                    "parameters": [path_parameter("owner")],
                    "responses": {
                        "200": response_with_etag("Stream ownership set", schema_ref("StreamSetResource")),
                        "400": error_response("Invalid owner"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream set not found"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "put": {
                    "operationId": "applyStreamSet",
                    "description": "Atomically applies the streams and optionally prunes omitted streams owned by this set. Exactly one of If-Match or If-None-Match is required",
                    "parameters": [
                        path_parameter("owner"),
                        header_parameter(
                            "If-Match",
                            "Current ETag when updating an existing stream set",
                            false,
                        ),
                        header_parameter(
                            "If-None-Match",
                            "Use * when creating a stream set that must not already exist",
                            false,
                        )
                    ],
                    "requestBody": request_body("StreamSetApply"),
                    "responses": {
                        "202": response_with_etag("Stream set accepted", schema_ref("StreamSetAccepted")),
                        "400": error_response("Invalid stream set"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream ownership conflict, or a stream can plan a hop id another can"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence or encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_PLANS: {
                "post": {
                    "operationId": "planStream",
                    "requestBody": request_body("StreamDefinition"),
                    "responses": {
                        "200": response("Stream placement plan", Some(schema_ref("StreamPlan"))),
                        "400": error_response("Invalid stream"),
                        "401": error_response("Authentication failed"),
                        "500": error_response("Encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STATUS: {
                "get": {
                    "operationId": "getStatus",
                    "responses": {
                        "200": response("Control-plane status", Some(schema_ref("StatusResponse"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            }
        }),
        components(&[
            ("ApiError", error),
            ("NodeList", schema::<Vec<NodeDescriptor>>()),
            ("StatusResponse", schema::<StatusResponse>()),
            ("StreamAccepted", schema::<StreamAccepted>()),
            ("StreamDefinition", schema::<StreamDefinition>()),
            ("StreamEndpoints", schema::<StreamEndpoints>()),
            ("StreamPlan", schema::<StreamPlan>()),
            ("StreamResource", schema::<StreamResource>()),
            ("StreamList", schema::<Vec<StreamResource>>()),
            ("StreamSetAccepted", schema::<StreamSetAccepted>()),
            ("StreamSetApply", schema::<StreamSetApply>()),
            ("StreamSetResource", schema::<StreamSetResource>()),
            ("StreamSetList", schema::<Vec<StreamSetResource>>()),
        ]),
    )
}

#[must_use]
pub fn southbound_openapi() -> Value {
    let error = schema::<ApiError>();
    common_document(
        "open-weave southbound API",
        json!({
            ROUTE_NODES: {
                "get": {
                    "operationId": "listNodes",
                    "responses": {
                        "200": response("Registered nodes", Some(schema_ref("NodeList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_REGISTER: {
                "post": {
                    "operationId": "registerNode",
                    "requestBody": request_body("NodeRegistration"),
                    "responses": {
                        "202": response("Node accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid registration"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "409": error_response("Incompatible protocol version"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_HEARTBEAT: {
                "post": {
                    "operationId": "heartbeatNode",
                    "parameters": [path_parameter("node_id")],
                    "requestBody": request_body("NodeHeartbeat"),
                    "responses": {
                        "202": response("Heartbeat accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid heartbeat"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "404": error_response("Node not found"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_DESIRED: {
                "get": {
                    "operationId": "getDesiredHops",
                    "parameters": [path_parameter("node_id")],
                    "responses": {
                        "200": response("Desired hops", Some(schema_ref("DesiredHopList"))),
                        "400": error_response("Invalid node id"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "404": error_response("Node not reconciled yet"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_ENDPOINTS: {
                "get": {
                    "operationId": "listEndpoints",
                    "responses": {
                        "200": response("Discovered endpoints", Some(schema_ref("EndpointList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STATE: {
                "get": {
                    "operationId": "getObservedState",
                    "responses": {
                        "200": response("Observed state", Some(schema_ref("ObservedState"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "503": error_response("No controller holds the lease")
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
    fn openapi_documents_cover_the_api_routes() {
        let north = northbound_openapi();
        let south = southbound_openapi();
        assert_eq!(north["openapi"], "3.1.0");
        assert_eq!(south["openapi"], "3.1.0");
        assert_eq!(north["security"][0]["bearerAuth"], json!([]));
        assert_eq!(south["security"][0]["bearerAuth"], json!([]));
        assert_eq!(
            north["paths"]["/streams/{name}"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["delete", "get"]
        );

        let north_paths: BTreeMap<_, _> = north["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            north_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/nodes",
                "/status",
                "/stream-plans",
                "/stream-sets",
                "/stream-sets/{owner}",
                "/streams",
                "/streams/{name}",
                "/streams/{name}/endpoints"
            ]
        );

        let south_paths: BTreeMap<_, _> = south["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            south_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/endpoints",
                "/nodes",
                "/nodes/register",
                "/nodes/{node_id}/desired",
                "/nodes/{node_id}/heartbeat",
                "/state"
            ]
        );
    }

    #[test]
    fn every_proxied_operation_can_answer_that_no_controller_leads() {
        for document in [northbound_openapi(), southbound_openapi()] {
            for (path, operations) in document["paths"].as_object().unwrap() {
                for (method, operation) in operations.as_object().unwrap() {
                    assert!(
                        operation["responses"]["503"].is_object(),
                        "{method} {path} lists no 503"
                    );
                }
            }
        }
    }

    #[test]
    fn stream_schema_carries_the_resource_id_rule() {
        let schema = serde_json::to_string(&schema::<StreamDefinition>()).unwrap();
        assert!(schema.contains("maxLength"));
        assert!(schema.contains("^[a-z0-9]"));
    }

    #[test]
    fn northbound_contract_exposes_node_inventory() {
        let document = northbound_openapi();
        let nodes = &document["paths"]["/nodes"]["get"];

        assert_eq!(nodes["operationId"], "listNodes");
        assert_eq!(
            nodes["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/NodeList"
        );
        assert!(nodes["responses"]["401"].is_object());
        assert!(nodes["responses"]["502"].is_object());
        assert!(document["components"]["schemas"]["NodeList"].is_object());
    }

    #[test]
    fn northbound_contract_exposes_resource_revisions() {
        let document = northbound_openapi();
        let apply = &document["paths"]["/streams"]["post"];
        let stream = &document["paths"]["/streams/{name}"];

        assert_eq!(
            stream["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/StreamResource"
        );
        assert!(stream["get"]["responses"]["200"]["headers"]["ETag"].is_object());
        assert!(apply["responses"]["202"]["headers"]["ETag"].is_object());
        assert!(apply["responses"]["412"].is_object());
        assert!(apply["responses"]["428"].is_object());
        assert_eq!(stream["delete"]["parameters"][1]["name"], "If-Match");
        assert_eq!(stream["delete"]["parameters"][1]["required"], true);
    }

    #[test]
    fn northbound_contract_exposes_atomic_stream_sets() {
        let document = northbound_openapi();
        let sets = &document["paths"]["/stream-sets"];
        let set = &document["paths"]["/stream-sets/{owner}"];

        assert!(sets["get"].is_object());
        assert_eq!(set["get"]["parameters"][0]["name"], "owner");
        assert!(set["get"]["responses"]["200"]["headers"]["ETag"].is_object());
        assert_eq!(
            set["put"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/StreamSetApply"
        );
        assert!(set["put"]["responses"]["202"]["headers"]["ETag"].is_object());
        assert!(set["put"]["responses"]["409"].is_object());
        assert!(set["put"]["responses"]["412"].is_object());
        assert!(set["put"]["responses"]["428"].is_object());
    }

    #[test]
    fn condition_transition_time_is_an_rfc3339_schema_string() {
        let schema = serde_json::to_value(schema::<StatusResponse>()).unwrap();
        let rendered = serde_json::to_string(&schema).unwrap();
        assert!(rendered.contains("last_transition_time"));
        assert!(rendered.contains("date-time"));
    }
}
