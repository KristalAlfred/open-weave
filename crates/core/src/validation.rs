use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    FormatConstraint, MediaFormat, NodeDescriptor, Passphrase, SignallingEndpoint, SrtEndpoint,
    StreamDefinition, StreamDestination, StreamTransport,
};

pub const RESOURCE_ID_MAX_LEN: usize = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResourceIdError {
    #[error("must be between 1 and {RESOURCE_ID_MAX_LEN} characters")]
    InvalidLength,
    #[error(
        "must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
    )]
    InvalidCharacters,
}

pub fn validate_resource_id(value: &str) -> Result<(), ResourceIdError> {
    if value.is_empty() || value.chars().count() > RESOURCE_ID_MAX_LEN {
        return Err(ResourceIdError::InvalidLength);
    }

    let mut bytes = value.bytes();
    let first = bytes.next().expect("non-empty resource id");
    let last = value.as_bytes()[value.len() - 1];
    if !is_alphanumeric(first)
        || !is_alphanumeric(last)
        || !bytes.all(|byte| is_alphanumeric(byte) || byte == b'-')
    {
        return Err(ResourceIdError::InvalidCharacters);
    }

    Ok(())
}

fn is_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}

impl ValidationIssue {
    pub fn new(
        field: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

#[must_use]
pub fn resource_id_issue(field: &str, label: &str, error: ResourceIdError) -> ValidationIssue {
    let code = match error {
        ResourceIdError::InvalidLength => "invalid_length",
        ResourceIdError::InvalidCharacters => "invalid_characters",
    };
    ValidationIssue::new(field, code, format!("{label} {error}"))
}

#[must_use]
pub fn validate_stream(stream: &StreamDefinition) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    validate_id(&stream.name, "name", "stream name", &mut issues);
    if stream.destinations.is_empty() {
        issues.push(ValidationIssue::new(
            "destinations",
            "required",
            "stream must have at least one destination",
        ));
    }

    validate_transport(&stream.source, "source", true, &mut issues);
    let mut destination_ids = HashSet::new();
    for (index, destination) in stream.destinations.iter().enumerate() {
        let path = format!("destinations[{index}]");
        validate_id(
            &destination.id,
            &format!("{path}.id"),
            "destination id",
            &mut issues,
        );
        if !destination_ids.insert(&destination.id) {
            issues.push(ValidationIssue::new(
                format!("{path}.id"),
                "duplicate",
                "destination id must be unique within the stream",
            ));
        }
        validate_transport(&destination.endpoint, &path, false, &mut issues);
        validate_paths(destination, &path, &mut issues);
    }

    issues
}

fn validate_paths(destination: &StreamDestination, path: &str, issues: &mut Vec<ValidationIssue>) {
    let field = format!("{path}.paths");
    if !(1..=2).contains(&destination.paths) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_range",
            "paths must be 1 or 2",
        ));
        return;
    }
    if destination.paths == 1 {
        return;
    }
    let StreamTransport::Srt(endpoint) = &destination.endpoint else {
        return;
    };
    if endpoint.remote.is_some() {
        issues.push(ValidationIssue::new(
            field,
            "not_allowed",
            "a remote destination has no receiver to merge two paths",
        ));
    } else if !endpoint.via.is_empty() {
        issues.push(ValidationIssue::new(
            field,
            "not_allowed",
            "a destination with two paths must not pin via",
        ));
    }
}

#[must_use]
pub fn validate_node(node: &NodeDescriptor) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    validate_id(&node.id, "node.id", "node id", &mut issues);
    let mut profile_ids = HashSet::new();
    for (index, profile) in node.capabilities.hop_profiles.iter().enumerate() {
        let field = format!("node.capabilities.hop_profiles[{index}].id");
        validate_id(&profile.id, &field, "profile id", &mut issues);
        if !profile_ids.insert(&profile.id) {
            issues.push(ValidationIssue::new(
                field,
                "duplicate",
                "profile id must be unique within the node",
            ));
        }
        if profile.max_egresses == Some(0) {
            issues.push(ValidationIssue::new(
                format!("node.capabilities.hop_profiles[{index}].max_egresses"),
                "invalid_range",
                "max_egresses must be at least one",
            ));
        }
        if let Some(accepts) = &profile.accepts {
            validate_constraint(
                accepts,
                &format!("node.capabilities.hop_profiles[{index}].accepts"),
                &mut issues,
            );
        }
    }
    let mut attachment_ids = HashSet::new();
    for (index, attachment) in node.topology.attachments.iter().enumerate() {
        let base = format!("node.topology.attachments[{index}]");
        validate_id(
            &attachment.id,
            &format!("{base}.id"),
            "attachment id",
            &mut issues,
        );
        validate_id(
            &attachment.network,
            &format!("{base}.network"),
            "network id",
            &mut issues,
        );
        if !attachment_ids.insert(&attachment.id) {
            issues.push(ValidationIssue::new(
                format!("{base}.id"),
                "duplicate",
                "attachment id must be unique within the node",
            ));
        }
        if let Some(listener) = &attachment.listeners.srt {
            if listener.host.trim().is_empty() {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.srt.host"),
                    "blank",
                    "SRT listener host must not be blank",
                ));
            }
            if listener.port_range.start > listener.port_range.end {
                issues.push(ValidationIssue::new(
                    format!("{base}.listeners.srt.port_range"),
                    "invalid_range",
                    "port range start must not exceed end",
                ));
            }
        }
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
        StreamTransport::Device(endpoint) => {
            validate_id(
                &endpoint.node,
                &format!("{path}.device.node"),
                "node id",
                issues,
            );
            if let Some(network) = &endpoint.network {
                validate_id(
                    network,
                    &format!("{path}.device.network"),
                    "network id",
                    issues,
                );
            }
        }
        StreamTransport::Whip(endpoint) => {
            let path = format!("{path}.whip");
            if !is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "source_only",
                    "whip belongs on the source, not a destination",
                ));
            }
            validate_signalling(endpoint, &path, is_source, issues);
        }
        StreamTransport::Whep(endpoint) => {
            let path = format!("{path}.whep");
            if is_source {
                issues.push(ValidationIssue::new(
                    &path,
                    "destination_only",
                    "whep belongs on a destination, not the source",
                ));
            }
            validate_signalling(endpoint, &path, is_source, issues);
        }
    }
}

fn validate_signalling(
    endpoint: &SignallingEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_id(&endpoint.node, &format!("{path}.node"), "node id", issues);
    if let Some(network) = &endpoint.network {
        validate_id(network, &format!("{path}.network"), "network id", issues);
    }
    validate_format(
        endpoint.format.as_ref(),
        endpoint.accepts.as_ref(),
        path,
        is_source,
        issues,
    );
}

fn validate_srt(
    endpoint: &SrtEndpoint,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    validate_via(endpoint, path, is_source, issues);
    validate_format(
        endpoint.format.as_ref(),
        endpoint.accepts.as_ref(),
        path,
        is_source,
        issues,
    );
    if let Some(passphrase) = &endpoint.passphrase {
        validate_passphrase(passphrase, &format!("{path}.passphrase"), issues);
    }
    if let Some(network) = &endpoint.network {
        validate_id(network, &format!("{path}.network"), "network id", issues);
    }

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

    if let Some(node) = &endpoint.node {
        validate_id(node, &format!("{path}.node"), "node id", issues);
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
        validate_id(
            &remote.network,
            &format!("{path}.remote.network"),
            "network id",
            issues,
        );
        if endpoint.network.is_some() {
            issues.push(ValidationIssue::new(
                format!("{path}.network"),
                "not_allowed",
                "a remote endpoint declares its network inside remote",
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
        validate_id(node, &format!("{path}.via[{index}]"), "node id", issues);
        if !seen.insert(node) {
            issues.push(ValidationIssue::new(
                format!("{path}.via[{index}]"),
                "duplicate",
                "via must not repeat a node",
            ));
        }
    }
}

/// Checks a passphrase against libsrt's limits. The messages never quote the
/// value.
fn validate_passphrase(passphrase: &Passphrase, field: &str, issues: &mut Vec<ValidationIssue>) {
    let value = passphrase.expose();
    if !(Passphrase::MIN_LEN..=Passphrase::MAX_LEN).contains(&value.len()) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_length",
            format!(
                "passphrase must be between {} and {} bytes",
                Passphrase::MIN_LEN,
                Passphrase::MAX_LEN
            ),
        ));
    }
    if value.chars().any(char::is_control) {
        issues.push(ValidationIssue::new(
            field,
            "invalid_characters",
            "passphrase must not contain control characters",
        ));
    }
}

fn validate_id(value: &str, field: &str, label: &str, issues: &mut Vec<ValidationIssue>) {
    if let Err(error) = validate_resource_id(value) {
        issues.push(resource_id_issue(field, label, error));
    }
}

fn validate_format(
    format: Option<&MediaFormat>,
    accepts: Option<&FormatConstraint>,
    path: &str,
    is_source: bool,
    issues: &mut Vec<ValidationIssue>,
) {
    if is_source && accepts.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.accepts"),
            "destination_only",
            "accepts belongs on a destination, not the source",
        ));
    }
    if !is_source && format.is_some() {
        issues.push(ValidationIssue::new(
            format!("{path}.format"),
            "source_only",
            "format belongs on the source, not a destination",
        ));
    }
    if let Some(accepts) = accepts {
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
        SrtEndpoint, StreamDefinition, StreamDestination, StreamTransport, VideoConstraint,
    };

    use super::{
        RESOURCE_ID_MAX_LEN, ResourceIdError, ValidationIssue, validate_resource_id,
        validate_stream,
    };

    fn srt_node(node: &str) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        }
    }

    fn destination(id: &str, endpoint: StreamTransport) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
            paths: 1,
            endpoint,
        }
    }

    fn stream() -> StreamDefinition {
        StreamDefinition {
            name: "camera".to_string(),
            enabled: true,
            source: StreamTransport::Srt(srt_node("source")),
            destinations: vec![destination(
                "destination",
                StreamTransport::Srt(srt_node("destination")),
            )],
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
            destinations: vec![destination(
                "screen",
                StreamTransport::Device(NodeEndpoint {
                    node: "browser-destination".to_string(),
                    network: None,
                }),
            )],
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
            "destinations": [{ "id": "destination", "srt": { "node": "destination" } }]
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
            network: "internet".to_string(),
        });
        source.via = vec!["relay".to_string(), "relay".to_string()];
        source.accepts = Some(FormatConstraint::default());

        let issues = validate_stream(&invalid);
        assert_eq!(
            issues,
            vec![
                issue(
                    "name",
                    "invalid_characters",
                    "stream name must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
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
                issue(
                    "source.srt.node",
                    "invalid_length",
                    "node id must be between 1 and 63 characters",
                ),
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
            passphrase: None,
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
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.node = None;
        endpoint.remote = Some(RemoteAddr {
            host: String::new(),
            port: 9000,
            network: "internet".to_string(),
        });
        endpoint.format = Some(MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: None,
        });
        invalid.destinations.push(destination(
            "screen",
            StreamTransport::Device(NodeEndpoint {
                node: " ".to_string(),
                network: None,
            }),
        ));

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
                    "invalid_characters",
                    "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
            ]
        );
    }

    #[test]
    fn every_empty_constraint_list_is_reported() {
        let mut invalid = stream();
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.accepts = Some(FormatConstraint {
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
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!();
        };
        endpoint.via = vec![" ".to_string(), "relay".to_string(), "relay".to_string()];

        assert_eq!(
            validate_stream(&invalid),
            vec![
                issue(
                    "destinations[0].srt.via[0]",
                    "invalid_characters",
                    "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit",
                ),
                issue(
                    "destinations[0].srt.via[2]",
                    "duplicate",
                    "via must not repeat a node",
                ),
            ]
        );
    }

    #[test]
    fn resource_ids_have_one_url_safe_grammar() {
        for valid in ["a", "0", "camera-1", &"a".repeat(RESOURCE_ID_MAX_LEN)] {
            assert_eq!(validate_resource_id(valid), Ok(()), "{valid}");
        }

        for invalid in [
            "Camera", "camera_1", "camera/1", "camera.1", "-camera", "camera-", "café",
        ] {
            assert_eq!(
                validate_resource_id(invalid),
                Err(ResourceIdError::InvalidCharacters),
                "{invalid}"
            );
        }
        assert_eq!(
            validate_resource_id(""),
            Err(ResourceIdError::InvalidLength)
        );
        assert_eq!(
            validate_resource_id(&"a".repeat(RESOURCE_ID_MAX_LEN + 1)),
            Err(ResourceIdError::InvalidLength)
        );
    }

    #[test]
    fn every_manifest_resource_reference_uses_the_id_grammar() {
        let mut invalid = stream();
        invalid.name = "camera/main".to_string();
        let StreamTransport::Srt(source) = &mut invalid.source else {
            unreachable!()
        };
        source.node = Some("Source".to_string());
        source.network = Some("studio_lan".to_string());
        let StreamTransport::Srt(endpoint) = &mut invalid.destinations[0].endpoint else {
            unreachable!()
        };
        endpoint.node = Some("destination_one".to_string());
        endpoint.via = vec!["relay.one".to_string()];
        invalid.destinations.push(destination(
            "display/one",
            StreamTransport::Device(NodeEndpoint {
                node: "display/one".to_string(),
                network: None,
            }),
        ));

        let issues = validate_stream(&invalid);
        assert_eq!(
            issues
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                ("name", "invalid_characters"),
                ("source.srt.network", "invalid_characters"),
                ("source.srt.node", "invalid_characters"),
                ("destinations[0].srt.via[0]", "invalid_characters"),
                ("destinations[0].srt.node", "invalid_characters"),
                ("destinations[1].id", "invalid_characters"),
                ("destinations[1].device.node", "invalid_characters"),
            ]
        );
    }

    #[test]
    fn two_paths_need_a_receiver_and_no_via() {
        let mut two = stream();
        two.destinations[0].paths = 2;
        assert!(validate_stream(&two).is_empty());

        let mut out_of_range = stream();
        out_of_range.destinations[0].paths = 3;
        assert_eq!(
            validate_stream(&out_of_range),
            [issue(
                "destinations[0].paths",
                "invalid_range",
                "paths must be 1 or 2"
            )]
        );
        out_of_range.destinations[0].paths = 0;
        assert_eq!(validate_stream(&out_of_range).len(), 1);

        let mut remote = srt_node("unused");
        remote.node = None;
        remote.remote = Some(crate::RemoteAddr {
            host: "198.51.100.5".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });
        let mut to_remote = stream();
        to_remote.destinations = vec![StreamDestination {
            paths: 2,
            ..destination("uplink", StreamTransport::Srt(remote))
        }];
        assert_eq!(
            validate_stream(&to_remote),
            [issue(
                "destinations[0].paths",
                "not_allowed",
                "a remote destination has no receiver to merge two paths"
            )]
        );

        let mut pinned = srt_node("destination");
        pinned.via = vec!["relay".to_string()];
        let mut via = stream();
        via.destinations = vec![StreamDestination {
            paths: 2,
            ..destination("destination", StreamTransport::Srt(pinned))
        }];
        assert_eq!(
            validate_stream(&via),
            [issue(
                "destinations[0].paths",
                "not_allowed",
                "a destination with two paths must not pin via"
            )]
        );
    }
    fn signalling(node: &str) -> crate::SignallingEndpoint {
        crate::SignallingEndpoint {
            node: node.to_string(),
            network: None,
            format: None,
            accepts: None,
        }
    }

    #[test]
    fn whip_is_a_source_and_whep_a_destination() {
        let mut outside = stream();
        outside.source = StreamTransport::Whip(signalling("gateway"));
        outside.destinations = vec![destination(
            "monitor",
            StreamTransport::Whep(signalling("player-edge")),
        )];
        assert_eq!(validate_stream(&outside), []);

        let mut reversed = stream();
        reversed.source = StreamTransport::Whep(signalling("gateway"));
        reversed.destinations = vec![destination(
            "monitor",
            StreamTransport::Whip(signalling("player-edge")),
        )];
        assert_eq!(
            validate_stream(&reversed),
            [
                issue(
                    "source.whep",
                    "destination_only",
                    "whep belongs on a destination, not the source",
                ),
                issue(
                    "destinations[0].whip",
                    "source_only",
                    "whip belongs on the source, not a destination",
                ),
            ]
        );
    }

    #[test]
    fn whip_and_whep_fields_are_validated() {
        let mut invalid = stream();
        invalid.source = StreamTransport::Whip(crate::SignallingEndpoint {
            network: Some("Internet".to_string()),
            accepts: Some(FormatConstraint::default()),
            ..signalling("gateway")
        });
        invalid.destinations = vec![destination(
            "monitor",
            StreamTransport::Whep(crate::SignallingEndpoint {
                format: Some(MediaFormat {
                    container: Container::MpegTs,
                    video: None,
                    audio: None,
                }),
                ..signalling("player_edge")
            }),
        )];
        assert_eq!(
            validate_stream(&invalid)
                .iter()
                .map(|issue| (issue.field.as_str(), issue.code.as_str()))
                .collect::<Vec<_>>(),
            [
                ("source.whip.network", "invalid_characters"),
                ("source.whip.accepts", "destination_only"),
                ("destinations[0].whep.node", "invalid_characters"),
                ("destinations[0].whep.format", "source_only"),
            ]
        );
    }

    #[test]
    fn a_whip_source_parses_from_its_manifest_tag() {
        let parsed: StreamDefinition = serde_json::from_value(serde_json::json!({
            "name": "encoder",
            "source": { "whip": { "node": "strom-node-1", "network": "internet" } },
            "destinations": [{ "id": "monitor", "whep": { "node": "strom-node-2" } }]
        }))
        .unwrap();
        assert_eq!(parsed.source.kind(), "whip");
        assert_eq!(parsed.source.node(), Some("strom-node-1"));
        assert_eq!(parsed.source.network(), Some("internet"));
        assert_eq!(parsed.destinations[0].endpoint.kind(), "whep");
        assert!(
            serde_json::from_value::<StreamDefinition>(serde_json::json!({
                "name": "encoder",
                "source": { "whip": { "node": "strom-node-1", "url": "http://x" } },
                "destinations": [{ "id": "monitor", "whep": { "node": "strom-node-2" } }]
            }))
            .is_err(),
            "a manifest never carries an address"
        );
    }

    #[test]
    fn a_hop_profile_constraint_follows_the_accepts_rules() {
        let class = crate::HopEndpointClass::Transport(crate::TransportClass {
            transport: crate::Transport::Srt,
            roles: crate::RoleSet::both(),
        });
        let node = crate::NodeDescriptor {
            id: "strom-node-1".to_string(),
            endpoint: "http://strom-node-1:8091".to_string(),
            status: crate::NodeStatus::Ready,
            capabilities: crate::NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![crate::HopProfile {
                    id: "srt-forward".to_string(),
                    ingress: class.clone(),
                    egress: class,
                    max_egresses: None,
                    merge: false,
                    accepts: Some(FormatConstraint {
                        video: Some(crate::VideoConstraint {
                            codec: Some(Vec::new()),
                            ..crate::VideoConstraint::default()
                        }),
                        ..FormatConstraint::default()
                    }),
                }],
            },
            topology: crate::NodeTopology::default(),
        };
        assert_eq!(
            super::validate_node(&node),
            [issue(
                "node.capabilities.hop_profiles[0].accepts.video.codec",
                "empty",
                "accepts must not contain an empty list of values",
            )]
        );
    }
}

#[cfg(test)]
mod passphrase_tests {
    use super::*;
    use crate::{StreamDestination, StreamTransport};

    fn keyed(passphrase: &str) -> StreamDefinition {
        StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("source".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                passphrase: Some(Passphrase::new(passphrase)),
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("studio".to_string()),
                    remote: None,
                    via: Vec::new(),
                    network: None,
                    latency: None,
                    passphrase: None,
                    format: None,
                    accepts: None,
                }),
            }],
        }
    }

    #[test]
    fn passphrase_length_follows_libsrt() {
        for (length, valid) in [(9, false), (10, true), (80, true), (81, false)] {
            let issues = validate_stream(&keyed(&"k".repeat(length)));
            assert_eq!(issues.is_empty(), valid, "{length}: {issues:?}");
        }
    }

    #[test]
    fn a_rejected_passphrase_is_named_but_never_quoted() {
        let secret = "short-key";
        let issues = validate_stream(&keyed(secret));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].field, "source.srt.passphrase");
        assert_eq!(issues[0].code, "invalid_length");
        assert!(!issues[0].message.contains(secret));

        let issues = validate_stream(&keyed("line\nbreak-in-key"));
        assert_eq!(issues[0].code, "invalid_characters");
    }
}
