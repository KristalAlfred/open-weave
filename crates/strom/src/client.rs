//! Async HTTP client for Strom's flow API.

use reqwest::{Method, RequestBuilder};
use serde::Deserialize;
use serde_json::Value;
use weave_core::auth::Token;

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
    token: Option<Token>,
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
            token: None,
        }
    }

    /// Present `token` as a bearer credential on every request. `None` is the
    /// default and sends no `Authorization` header.
    #[must_use]
    pub fn with_token(mut self, token: Option<Token>) -> Self {
        self.token = token;
        self
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let request = self
            .http
            .request(method, format!("{}{path}", self.base_url));
        match &self.token {
            Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => request,
        }
    }

    pub async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
        let response = self
            .request(Method::GET, "/api/flows")
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<FlowListResponse>().await?.flows)
    }

    pub async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
        let response = self
            .request(Method::POST, "/api/flows")
            .json(spec)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json::<CreateFlowResponse>().await?.flow.id)
    }

    pub async fn start_flow(&self, id: &str) -> Result<(), StromError> {
        self.request(Method::POST, &format!("/api/flows/{id}/start"))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
        self.request(Method::DELETE, &format!("/api/flows/{id}"))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
        let response = self
            .request(Method::GET, &format!("/api/flows/{id}/srt-stats"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A stub Strom listening on a real socket, recording the `Authorization`
    /// header of the last request and answering with an empty flow list.
    async fn stub_strom() -> (String, Arc<Mutex<Option<String>>>) {
        use axum::extract::{Request, State};
        use axum::response::{IntoResponse, Response};
        use axum::{Json, Router};

        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        async fn record(
            State(captured): State<Arc<Mutex<Option<String>>>>,
            request: Request,
        ) -> Response {
            *captured.lock().unwrap() = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            Json(serde_json::json!({ "flows": [] })).into_response()
        }

        let app = Router::new().fallback(record).with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test]
    async fn presents_the_bearer_token_when_one_is_set() {
        let (url, captured) = stub_strom().await;
        let client = StromClient::new(url).with_token(Token::new("strom-secret"));

        client.list_flows().await.expect("list flows");

        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some("Bearer strom-secret")
        );
    }

    #[tokio::test]
    async fn sends_no_authorization_header_without_a_token() {
        let (url, captured) = stub_strom().await;
        let client = StromClient::new(url);

        client.list_flows().await.expect("list flows");

        assert_eq!(captured.lock().unwrap().as_deref(), None);
    }
}
