//! Deserialization structs for Strom's flow-list responses.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct FlowListResponse {
    #[serde(default)]
    pub flows: Vec<StromFlow>,
}

#[derive(Debug, Deserialize)]
pub struct StromFlow {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub running: bool,
    #[serde(default)]
    pub elements: Vec<StromElement>,
    #[serde(default)]
    pub blocks: Vec<StromBlock>,
}

#[derive(Debug, Deserialize)]
pub struct StromElement {
    #[serde(default)]
    pub element_type: String,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct StromBlock {
    #[serde(default)]
    pub block_definition_id: String,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
}
