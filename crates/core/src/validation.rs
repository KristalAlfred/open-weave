use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{FormatConstraint, SrtEndpoint, StreamDefinition, StreamTransport};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}

impl ValidationIssue {
    fn new(field: impl Into<String>, code: &str, message: &str) -> Self {
        Self {
            field: field.into(),
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

#[must_use]
pub fn validate_stream(stream: &StreamDefinition) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    if stream.name.trim().is_empty() {
        issues.push(ValidationIssue::new(
            "name",
            "blank",
            "stream name must not be empty",
        ));
    }
    if stream.destinations.is_empty() {
        issues.push(ValidationIssue::new(
            "destinations",
            "required",
            "stream must have at least one destination",
        ));
    }

    validate_transport(&stream.source, "source", true, &mut issues);
    for (index, destination) in stream.destinations.iter().enumerate() {
        validate_transport(
            destination,
            &format!("destinations[{index}]"),
            false,
            &mut issues,
        );
    }

    issues
}

fn validate_transport(
    transport: &StreamTransport,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    match transport {
        StreamTransport::Srt(endpoint) => {
            validate_srt(endpoint, &format!("{path}.srt"), is_source, issues);
        }
        StreamTransport::Device(endpoint) if endpoint.node.trim().is_empty() => {
            issues.push(ValidationIssue::new(
                format!("{path}.device.node"),
                "blank",
                "device node must not be empty",
            ));
        }
        StreamTransport::Device(_) => {}
    }
}

fn validate_srt(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_via(endpoint, path, is_source, issues);
    validate_format(endpoint, path, is_source, issues);

    match (&endpoint.node, &endpoint.remote) {
        (Some(_), Some(_)) => issues.push(ValidationIssue::new(
            path,
            "mutually_exclusive",
            "endpoint must set either node or remote, not both",
        )),
        (None, None) => issues.push(ValidationIssue::new(
            path,
            "required",
            "endpoint must set either node or remote",
        )),
        _ => {}
    }

    if endpoint
        .node
        .as_ref()
        .is_some_and(|node| node.trim().is_empty())
    {
        issues.push(ValidationIssue::new(
            format!("{path}.node"),
            "blank",
            "node must not be empty",
        ));
    }

    if let Some(remote) = &endpoint.remote {
        if is_source {
            issues.push(ValidationIssue::new(
                format!("{path}.remote"),
                "not_allowed",
                "source must be a node, not a remote endpoint",
            ));
        }
        if remote.host.trim().is_empty() {
            issues.push(ValidationIssue::new(
                format!("{path}.remote.host"),
                "blank",
                "remote host must not be empty",
            ));
        }
    }
}

fn validate_via(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_source && !endpoint.via.is_empty() {
        issues.push(ValidationIssue::new(
            format!("{path}.via"),
            "destination_only",
            "via belongs on a destination, not the source",
        ));
    }

    let mut seen = HashSet::new();
    for (index, node) in endpoint.via.iter().enumerate() {
        if node.trim().is_empty() {
            issues.push(ValidationIssue::new(
                format!("{path}.via[{index}]"),
                "blank",
                "via must not contain an empty node id",
            ));
        }
        if !seen.insert(node) {
            issues.push(ValidationIssue::new(
                format!("{path}.via[{index}]"),
                "duplicate",
                "via must not repeat a node",
            ));
        }
    }
}

fn validate_format(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_source && endpoint.accepts.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.accepts"),
            "destination_only",
            "accepts belongs on a destination, not the source",
        ));
    }
    if !is_source && endpoint.format.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.format"),
            "source_only",
            "format belongs on the source, not a destination",
        ));
    }
    if let Some(accepts) = &endpoint.accepts {
        validate_constraint(accepts, &format!("{path}.accepts"), issues);
    }
}

fn validate_constraint(accepts: &FormatConstraint, path: &str, issues: &mut Vec<ValidationIssue>) {
    fn check<T>(values: &Option<Vec<T>>, path: String, issues: &mut Vec<ValidationIssue>) {
        if values.as_ref().is_some_and(Vec::is_empty) {
            issues.push(ValidationIssue::new(
                path,
                "empty",
                "accepts must not contain an empty list of values",
            ));
        }
    }

    check(&accepts.container, format!("{path}.container"), issues);
    if let Some(video) = &accepts.video {
        check(&video.codec, format!("{path}.video.codec"), issues);
        check(&video.width, format!("{path}.video.width"), issues);
        check(&video.height, format!("{path}.video.height"), issues);
        check(&video.framerate, format!("{path}.video.framerate"), issues);
        check(
            &video.chroma_subsampling,
            format!("{path}.video.chroma_subsampling"),
            issues,
        );
    }
    if let Some(audio) = &accepts.audio {
        check(&audio.codec, format!("{path}.audio.codec"), issues);
        check(
            &audio.sample_rate,
            format!("{path}.audio.sample_rate"),
            issues,
        );
        check(&audio.channels, format!("{path}.audio.channels"), issues);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AudioConstraint, Container, FormatConstraint, MediaFormat, NodeEndpoint, RemoteAddr,
        SrtEndpoint, StreamDefinition, StreamTransport, VideoConstraint,
    };

    use super::{ValidationIssue, validate_stream};

    fn srt_node(node: &str) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            format: None,
            accepts: None,
        }
    }

    fn stream() -> StreamDefinition {
        StreamDefinition {
            name: "camera".to_string(),
            enabled: true,
            source: StreamTransport::Srt(srt_node("source")),
            destinations: vec![StreamTransport::Srt(srt_node("destination"))],
        }
    }

    fn issue(field: &str, code: &str, message: &str) -> ValidationIssue {
        ValidationIssue {
            field: field.to_string(),
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    #[test]
    fn valid_srt_and_device_streams_have_no_issues() {
        assert!(validate_stream(&stream()).is_empty());

        let device_stream = StreamDefinition {
            name: "browser".to_string(),
            enabled: true,
            source: StreamTransport::Device(NodeEndpoint {
                node: "browser-source".to_string(),
                network: None,
            }),
            destinations: vec![StreamTransport::Device(NodeEndpoint {
                node: "browser-destination".to_string(),
                network: None,
            })],
        };
        assert!(validate_stream(&device_stream).is_empty());
    }

    #[test]
    fn stream_definition_rejects_unknown_fields() {
        let parsed = serde_json::from_value::<StreamDefinition>(serde_json::json!({
            "name": "camera",
            "enabled": true,
            "enabld": true,
            "source": { "srt": { "node": "source" } },
            "destinations": [{ "srt": { "node": "destination" } }]
        }));

        assert!(
            parsed
                .unwrap_err()
                .to_string()
                .contains("unknown field `enabld`")
        );
    }

    #[test]
    fn validation_collects_issues_with_stable_paths() {
        let mut invalid = stream();
        invalid.name = "  ".to_string();
        let StreamTransport::Srt(source) = &mut invalid.source else {
            unreachable!();
        };
        source.node = Some(String::new());
        source.remote = Some(RemoteAddr {
            host: " ".to_string(),
            port: 9000,
        });
        source.via = vec!["relay".to_string(), "relay".to_string()];
        source.accepts = Some(FormatConstraint::default());

        let issues = validate_stream(&invalid);
        assert_eq!(
            issues,
            vec![
                issue("name", "blank", "stream name must not be empty"),
                issue(
                    "source.srt.via",
                    "destination_only",
                    "via belongs on a destination, not the source",
                ),
                issue(
                    "source.srt.via[1]",
                    "duplicate",
                    "via must not repeat a node",
                ),
                issue(
                    "source.srt.accepts",
                    "destination_only",
                    "accepts belongs on a destination, not the source",
                ),
                issue(
                    "source.srt",
                    "mutually_exclusive",
                    "endpoint must set either node or remote, not both",
                ),
                issue("source.srt.node", "blank", "node must not be empty"),
                issue(
                    "source.srt.remote",
                    "not_allowed",
                    "source must be a node, not a remote endpoint",
                ),
                issue(
                    "source.srt.remote.host",
                    "blank",
                    "remote host must not be empty",
                ),
            ]
        );
    }

    #[test]
    fn missing_destination_and_endpoint_identity_are_reported() {
        let mut invalid = stream();
        invalid.destinations.clear();
        invalid.source = StreamTransport::Srt(SrtEndpoint {
            node: None,
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            format: None,
            accepts: None,
        });

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations",
                    "required",
                    "stream must have at least one destination",
                ),
                issue(
                    "source.srt",
                    "required",
                    "endpoint must set either node or remote",
                ),
            ]
        );
    }

    #[test]
    fn destination_roles_and_node_fields_are_validated() {
        let mut invalid = stream();
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0] else {
            unreachable!();
        };
        destination.node = None;
        destination.remote = Some(RemoteAddr {
            host: String::new(),
            port: 9000,
        });
        destination.format = Some(MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: None,
        });
        invalid
            .destinations
            .push(StreamTransport::Device(NodeEndpoint {
                node: " ".to_string(),
                network: None,
            }));

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations[0].srt.format",
                    "source_only",
                    "format belongs on the source, not a destination",
                ),
                issue(
                    "destinations[0].srt.remote.host",
                    "blank",
                    "remote host must not be empty",
                ),
                issue(
                    "destinations[1].device.node",
                    "blank",
                    "device node must not be empty",
                ),
            ]
        );
    }

    #[test]
    fn every_empty_constraint_list_is_reported() {
        let mut invalid = stream();
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0] else {
            unreachable!();
        };
        destination.accepts = Some(FormatConstraint {
            container: Some(Vec::new()),
            video: Some(VideoConstraint {
                codec: Some(Vec::new()),
                width: Some(Vec::new()),
                height: Some(Vec::new()),
                framerate: Some(Vec::new()),
                chroma_subsampling: Some(Vec::new()),
            }),
            audio: Some(AudioConstraint {
                codec: Some(Vec::new()),
                sample_rate: Some(Vec::new()),
                channels: Some(Vec::new()),
            }),
        });

        let issues = validate_stream(&invalid);
        let fields: Vec<&str> = issues.iter().map(|issue| issue.field.as_str()).collect();
        assert_eq!(
            fields,
            vec![
                "destinations[0].srt.accepts.container",
                "destinations[0].srt.accepts.video.codec",
                "destinations[0].srt.accepts.video.width",
                "destinations[0].srt.accepts.video.height",
                "destinations[0].srt.accepts.video.framerate",
                "destinations[0].srt.accepts.video.chroma_subsampling",
                "destinations[0].srt.accepts.audio.codec",
                "destinations[0].srt.accepts.audio.sample_rate",
                "destinations[0].srt.accepts.audio.channels",
            ]
        );
        assert!(issues.iter().all(|issue| issue.code == "empty"
            && issue.message == "accepts must not contain an empty list of values"));
    }

    #[test]
    fn blank_and_duplicate_via_entries_are_each_identified() {
        let mut invalid = stream();
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0] else {
            unreachable!();
        };
        destination.via = vec![" ".to_string(), "relay".to_string(), "relay".to_string()];

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations[0].srt.via[0]",
                    "blank",
                    "via must not contain an empty node id",
                ),
                issue(
                    "destinations[0].srt.via[2]",
                    "duplicate",
                    "via must not repeat a node",
                ),
            ]
        );
    }
}
