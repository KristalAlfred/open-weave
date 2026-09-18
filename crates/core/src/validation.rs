use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{FormatConstraint, NodeDescriptor, SrtEndpoint, StreamDefinition, StreamTransport};

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
    }

    issues
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

fn validate_id(value: &str, field: &str, label: &str, issues: &mut Vec<ValidationIssue>) {
    if let Err(error) = validate_resource_id(value) {
        issues.push(resource_id_issue(field, label, error));
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
