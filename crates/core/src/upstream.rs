//! The controllers a stateless proxy forwards to. With several sharing one
//! Postgres, the one holding the lease answers and the others say
//! `503 not_leader`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;

use crate::{ApiError, ApiErrorCode};

/// A comma-separated list of controller base URLs.
pub const CONTROLLER_URL_VAR: &str = "WEAVE_CONTROLLER_URL";
pub const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:8082";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum ControllersError {
    #[error("{CONTROLLER_URL_VAR} names no controller")]
    Empty,
    #[error("building the controller HTTP client")]
    Client(#[source] reqwest::Error),
}

/// A request no controller answered.
#[derive(Debug, thiserror::Error)]
#[error("{url}: {source}")]
pub struct Unanswered {
    pub url: String,
    #[source]
    pub source: reqwest::Error,
}

/// A controller's response, read in full.
#[derive(Debug)]
pub struct Answer {
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Bytes,
}

impl Answer {
    async fn read(response: reqwest::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers().clone(),
            body: response.bytes().await.unwrap_or_default(),
        }
    }

    fn is_not_leader(&self) -> bool {
        self.status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            && serde_json::from_slice::<ApiError>(&self.body)
                .is_ok_and(|error| error.code == ApiErrorCode::NotLeader)
    }
}

pub struct Controllers {
    http: reqwest::Client,
    urls: Vec<String>,
    leader: AtomicUsize,
}

impl Controllers {
    /// The controllers [`CONTROLLER_URL_VAR`] lists, or
    /// [`DEFAULT_CONTROLLER_URL`] when it is unset.
    pub fn from_env() -> Result<Self, ControllersError> {
        Self::new(
            std::env::var(CONTROLLER_URL_VAR)
                .as_deref()
                .unwrap_or(DEFAULT_CONTROLLER_URL),
        )
    }

    pub fn new(list: &str) -> Result<Self, ControllersError> {
        let urls: Vec<String> = list
            .split(',')
            .map(|url| url.trim().trim_end_matches('/'))
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect();
        if urls.is_empty() {
            return Err(ControllersError::Empty);
        }
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(ControllersError::Client)?;
        Ok(Self {
            http,
            urls,
            leader: AtomicUsize::new(0),
        })
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// Send `method path`, shaped by `build`, to the controller that answered
    /// last, then to each other one in turn. It moves on only when a controller
    /// cannot be connected to or answers `503 not_leader`; neither has acted on
    /// the request, so a write is never sent twice. Any other answer is
    /// returned, and so is `not_leader` when no controller leads. A request
    /// that fails after connecting is not retried.
    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        build: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    ) -> Result<Answer, Unanswered> {
        let first = self.leader.load(Ordering::Relaxed);
        let mut standby = None;
        let mut unreachable = None;
        for offset in 0..self.urls.len() {
            let index = (first + offset) % self.urls.len();
            let url = format!("{}{path}", self.urls[index]);
            let response = match build(self.http.request(method.clone(), &url)).send().await {
                Ok(response) => response,
                Err(source) if source.is_connect() => {
                    tracing::debug!(%source, %url, "controller unreachable; trying the next");
                    unreachable = Some(Unanswered { url, source });
                    continue;
                }
                Err(source) => return Err(Unanswered { url, source }),
            };
            let answer = Answer::read(response).await;
            if answer.is_not_leader() {
                tracing::debug!(%url, "controller does not lead; trying the next");
                standby = Some(answer);
                continue;
            }
            self.leader.store(index, Ordering::Relaxed);
            return Ok(answer);
        }
        match (standby, unreachable) {
            (Some(answer), _) => Ok(answer),
            (None, Some(unanswered)) => Err(unanswered),
            (None, None) => unreachable!("a controller list is never empty"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::*;

    struct Stub {
        url: String,
        hits: Arc<AtomicUsize>,
    }

    impl Stub {
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    /// A controller answering every request with `status`, and with an API
    /// error carrying `code` when there is one.
    async fn stub(status: StatusCode, code: Option<ApiErrorCode>) -> Stub {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new().fallback({
            let hits = hits.clone();
            move || {
                hits.fetch_add(1, Ordering::SeqCst);
                async move {
                    match code {
                        Some(code) => ApiError::new(code, "stub").response(status),
                        None => (status, "{}").into_response(),
                    }
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Stub { url, hits }
    }

    /// An address nothing listens on.
    async fn refusing() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    fn controllers(urls: &[&str]) -> Controllers {
        Controllers::new(&urls.join(",")).unwrap()
    }

    async fn post(controllers: &Controllers) -> Result<Answer, Unanswered> {
        controllers
            .send(reqwest::Method::POST, "/streams", |request| {
                request.body("{}")
            })
            .await
    }

    fn not_leader() -> Option<ApiErrorCode> {
        Some(ApiErrorCode::NotLeader)
    }

    #[test]
    fn the_list_is_comma_separated_and_never_empty() {
        let controllers = Controllers::new(" http://a:8082/, ,http://b:8082 ").unwrap();
        assert_eq!(controllers.urls(), ["http://a:8082", "http://b:8082"]);
        assert!(matches!(
            Controllers::new(" , "),
            Err(ControllersError::Empty)
        ));
    }

    #[tokio::test]
    async fn a_request_moves_past_a_controller_it_cannot_connect_to() {
        let leader = stub(StatusCode::ACCEPTED, None).await;
        let controllers = controllers(&[&refusing().await, &leader.url]);

        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!(leader.hits(), 1);
    }

    #[tokio::test]
    async fn a_request_moves_past_a_standby_and_goes_to_the_leader_first_after() {
        let standby = stub(StatusCode::SERVICE_UNAVAILABLE, not_leader()).await;
        let leader = stub(StatusCode::ACCEPTED, None).await;
        let controllers = controllers(&[&standby.url, &leader.url]);

        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!((standby.hits(), leader.hits()), (1, 2));
    }

    #[tokio::test]
    async fn any_other_answer_is_returned_without_trying_the_next() {
        for (status, code) in [
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Some(ApiErrorCode::PersistenceFailed),
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(ApiErrorCode::StreamNotReady),
            ),
            (StatusCode::SERVICE_UNAVAILABLE, None),
            (
                StatusCode::PRECONDITION_FAILED,
                Some(ApiErrorCode::PreconditionFailed),
            ),
        ] {
            let first = stub(status, code).await;
            let second = stub(StatusCode::ACCEPTED, None).await;
            let controllers = controllers(&[&first.url, &second.url]);

            assert_eq!(post(&controllers).await.unwrap().status, status, "{code:?}");
            assert_eq!((first.hits(), second.hits()), (1, 0), "{status} {code:?}");
        }
    }

    #[tokio::test]
    async fn with_no_leader_the_standby_answer_is_returned() {
        let standby = stub(StatusCode::SERVICE_UNAVAILABLE, not_leader()).await;
        let controllers = controllers(&[&refusing().await, &standby.url]);

        let answer = post(&controllers).await.unwrap();
        assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            serde_json::from_slice::<ApiError>(&answer.body)
                .unwrap()
                .code,
            ApiErrorCode::NotLeader
        );
    }

    #[tokio::test]
    async fn with_no_controller_reachable_the_last_one_tried_is_named() {
        let last = refusing().await;
        let controllers = controllers(&[&refusing().await, &last]);

        let unanswered = post(&controllers).await.unwrap_err();
        assert_eq!(unanswered.url, format!("{last}/streams"));
        assert!(unanswered.source.is_connect());
    }
}
