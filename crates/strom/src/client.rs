//! Async HTTP client for Strom's flow API.

use serde::Deserialize;
use serde_json::Value;

use crate::flow::{FlowListResponse, StromFlow};
use crate::spec::{FlowSpec, MappingError};

#[derive(Debug, thiserror::Error)]
pub enum StromError {
    #[error("strom request failed")]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    Mapping(#[from] MappingError),
}

#[derive(Clone)]
pub struct StromClient {
    base_url: String,
    http: reqwest::Client,
}

impl StromClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_client(base_url, reqwest::Client::new())
    }

    #[must_use]
    pub fn with_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        }
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
        let response = self
            .http
            .get(format!("{}/api/flows", self.base_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<FlowListResponse>().await?.flows)
    }

    pub async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
        let response = self
            .http
            .post(format!("{}/api/flows", self.base_url))
            .json(spec)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<CreateFlowResponse>().await?.flow.id)
    }

    pub async fn start_flow(&self, id: &str) -> Result<(), StromError> {
        self.http
            .post(format!("{}/api/flows/{id}/start", self.base_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
        let response = self
            .http
            .get(format!("{}/api/flows/{id}/srt-stats", self.base_url))
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<Value>().await?)
    }
}

#[derive(Debug, Deserialize)]
struct CreateFlowResponse {
    flow: CreatedFlow,
}

#[derive(Debug, Deserialize)]
struct CreatedFlow {
    id: String,
}
