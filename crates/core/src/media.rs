//! What is on the wire, and what an endpoint will accept.
//!
//! Two shapes, deliberately distinct. A [`MediaFormat`] is fixated: every field
//! has one value, and it describes media that exists. A [`FormatConstraint`] is
//! partially specified: each field carries the set of values an endpoint accepts,
//! and an absent field constrains nothing.
//!
//! That split is borrowed from GStreamer caps, and only the algebra transfers.
//! GStreamer negotiates at runtime, in one process, downstream-first over a
//! shared bus; none of that exists across a control plane. What does transfer is
//! caps as constraint sets that a concrete format is checked against — which is
//! all planning needs to answer "does this endpoint need a conversion, and if so
//! which one".

use std::fmt;

use serde::{Deserialize, Serialize};

/// A fully specified description of the media on a link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaFormat {
    pub container: Container,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioFormat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Container {
    MpegTs,
    Rtp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VideoFormat {
    pub codec: VideoCodec,
    pub width: u32,
    pub height: u32,
    pub framerate: Framerate,
    pub chroma_subsampling: ChromaSubsampling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoCodec {
    H264,
    H265,
    Av1,
    Vp9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChromaSubsampling {
    Yuv420,
    Yuv422,
    Yuv444,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudioFormat {
    pub codec: AudioCodec,
    /// Samples per second. The field a sample-rate conversion changes.
    pub sample_rate: u32,
    pub channels: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCodec {
    Aac,
    Opus,
    Mp2,
    PcmS16,
    PcmS24,
}

/// Frames per second as a rational, because broadcast rates are not integers:
/// 29.97 is exactly 30000/1001 and rounding it loses the distinction from 30.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Framerate {
    pub numerator: u32,
    pub denominator: u32,
}

impl Framerate {
    #[must_use]
    pub fn new(numerator: u32, denominator: u32) -> Self {
        Self {
            numerator,
            denominator,
        }
    }
}

impl fmt::Display for Framerate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.numerator, self.denominator)
    }
}

/// The media an endpoint accepts. Every field is optional; an absent one accepts
/// anything, so an empty constraint is satisfied by any format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct FormatConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<Vec<Container>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoConstraint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct VideoConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<Vec<VideoCodec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<Vec<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<Vec<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub framerate: Option<Vec<Framerate>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chroma_subsampling: Option<Vec<ChromaSubsampling>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AudioConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<Vec<AudioCodec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<Vec<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<u8>>,
}

/// One field of a format that an endpoint will not accept.
///
/// Carrying the field and both sides rather than a rendered sentence keeps the
/// reason usable by more than a log line: it is what a conversion planner will
/// read to decide which transform to look for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mismatch {
    /// Dotted path of the offending field, e.g. `audio.sample_rate`.
    pub field: String,
    pub actual: String,
    /// Values the endpoint accepts, or the track it required and did not get.
    pub accepted: String,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} is {} but accepts {}",
            self.field, self.actual, self.accepted
        )
    }
}

/// A destination whose declared constraint the source format does not satisfy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatConflict {
    /// Index of the destination in the stream's list, as written in the manifest.
    pub destination: usize,
    pub mismatches: Vec<Mismatch>,
}

impl fmt::Display for FormatConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let detail: Vec<String> = self.mismatches.iter().map(ToString::to_string).collect();
        write!(
            f,
            "destination {} cannot accept the source format: {}",
            self.destination,
            detail.join("; ")
        )
    }
}

impl FormatConstraint {
    /// Whether any field of `format` falls outside what this constraint accepts.
    #[must_use]
    pub fn satisfied_by(&self, format: &MediaFormat) -> bool {
        self.mismatches(format).is_empty()
    }

    /// Every field of `format` this constraint rejects, in a stable order.
    ///
    /// Constraining a track the format does not carry is itself a mismatch: an
    /// endpoint asking for 48 kHz audio is not served by a video-only stream, and
    /// silently passing that would defeat the point of declaring it. A track the
    /// constraint says nothing about is never a mismatch, however present.
    #[must_use]
    pub fn mismatches(&self, format: &MediaFormat) -> Vec<Mismatch> {
        let mut out = Vec::new();

        check(
            "container",
            &format.container,
            self.container.as_deref(),
            &mut out,
        );

        if let Some(video) = &self.video {
            match &format.video {
                None => out.push(missing_track("video")),
                Some(actual) => {
                    check(
                        "video.codec",
                        &actual.codec,
                        video.codec.as_deref(),
                        &mut out,
                    );
                    check(
                        "video.width",
                        &actual.width,
                        video.width.as_deref(),
                        &mut out,
                    );
                    check(
                        "video.height",
                        &actual.height,
                        video.height.as_deref(),
                        &mut out,
                    );
                    check(
                        "video.framerate",
                        &actual.framerate,
                        video.framerate.as_deref(),
                        &mut out,
                    );
                    check(
                        "video.chroma_subsampling",
                        &actual.chroma_subsampling,
                        video.chroma_subsampling.as_deref(),
                        &mut out,
                    );
                }
            }
        }

        if let Some(audio) = &self.audio {
            match &format.audio {
                None => out.push(missing_track("audio")),
                Some(actual) => {
                    check(
                        "audio.codec",
                        &actual.codec,
                        audio.codec.as_deref(),
                        &mut out,
                    );
                    check(
                        "audio.sample_rate",
                        &actual.sample_rate,
                        audio.sample_rate.as_deref(),
                        &mut out,
                    );
                    check(
                        "audio.channels",
                        &actual.channels,
                        audio.channels.as_deref(),
                        &mut out,
                    );
                }
            }
        }

        out
    }
}

/// Record a mismatch when `actual` is outside `accepted`. An absent `accepted`
/// constrains nothing and never records one.
fn check<T>(field: &str, actual: &T, accepted: Option<&[T]>, out: &mut Vec<Mismatch>)
where
    T: PartialEq + fmt::Debug,
{
    let Some(accepted) = accepted else { return };
    if accepted.contains(actual) {
        return;
    }
    out.push(Mismatch {
        field: field.to_string(),
        actual: render(actual),
        accepted: accepted.iter().map(render).collect::<Vec<_>>().join(", "),
    });
}

fn missing_track(track: &str) -> Mismatch {
    Mismatch {
        field: track.to_string(),
        actual: "absent".to_string(),
        accepted: "a track".to_string(),
    }
}

/// Render a value the way a manifest spells it: enum variants are snake_case on
/// the wire, so `{:?}` alone would print `MpegTs` where the operator wrote
/// `mpeg_ts`.
fn render<T: fmt::Debug>(value: &T) -> String {
    let debug = format!("{value:?}");
    if debug.starts_with(|c: char| c.is_ascii_digit()) {
        return debug;
    }
    // Framerate and other structs Debug as `Name { .. }`; leave those alone.
    if debug.contains(['{', '(']) {
        return debug;
    }
    snake_case(&debug)
}

fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    for (i, ch) in name.char_indices() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_format() -> MediaFormat {
        MediaFormat {
            container: Container::MpegTs,
            video: Some(VideoFormat {
                codec: VideoCodec::H264,
                width: 1280,
                height: 720,
                framerate: Framerate::new(30, 1),
                chroma_subsampling: ChromaSubsampling::Yuv420,
            }),
            audio: Some(AudioFormat {
                codec: AudioCodec::Aac,
                sample_rate: 48_000,
                channels: 2,
            }),
        }
    }

    #[test]
    fn an_empty_constraint_accepts_anything() {
        assert!(FormatConstraint::default().satisfied_by(&source_format()));
    }

    #[test]
    fn a_matching_constraint_is_satisfied() {
        let constraint = FormatConstraint {
            container: Some(vec![Container::MpegTs]),
            audio: Some(AudioConstraint {
                codec: Some(vec![AudioCodec::Aac]),
                sample_rate: Some(vec![44_100, 48_000]),
                ..AudioConstraint::default()
            }),
            video: None,
        };
        assert!(constraint.satisfied_by(&source_format()));
    }

    #[test]
    fn a_rejected_sample_rate_names_the_field_and_both_sides() {
        let constraint = FormatConstraint {
            audio: Some(AudioConstraint {
                sample_rate: Some(vec![44_100]),
                ..AudioConstraint::default()
            }),
            ..FormatConstraint::default()
        };

        let mismatches = constraint.mismatches(&source_format());
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].field, "audio.sample_rate");
        assert_eq!(mismatches[0].actual, "48000");
        assert_eq!(mismatches[0].accepted, "44100");
        assert_eq!(
            mismatches[0].to_string(),
            "audio.sample_rate is 48000 but accepts 44100"
        );
    }

    #[test]
    fn every_offending_field_is_reported_not_just_the_first() {
        let constraint = FormatConstraint {
            container: Some(vec![Container::Rtp]),
            audio: Some(AudioConstraint {
                codec: Some(vec![AudioCodec::Opus]),
                sample_rate: Some(vec![44_100]),
                ..AudioConstraint::default()
            }),
            video: Some(VideoConstraint {
                codec: Some(vec![VideoCodec::H265]),
                chroma_subsampling: Some(vec![ChromaSubsampling::Yuv444]),
                ..VideoConstraint::default()
            }),
        };

        let fields: Vec<String> = constraint
            .mismatches(&source_format())
            .into_iter()
            .map(|m| m.field)
            .collect();
        assert_eq!(
            fields,
            vec![
                "container",
                "video.codec",
                "video.chroma_subsampling",
                "audio.codec",
                "audio.sample_rate"
            ]
        );
    }

    #[test]
    fn enum_values_render_the_way_a_manifest_spells_them() {
        let constraint = FormatConstraint {
            container: Some(vec![Container::Rtp]),
            ..FormatConstraint::default()
        };
        let mismatch = &constraint.mismatches(&source_format())[0];
        assert_eq!(mismatch.actual, "mpeg_ts");
        assert_eq!(mismatch.accepted, "rtp");
    }

    #[test]
    fn constraining_a_track_the_format_lacks_is_a_mismatch() {
        let audio_only = MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: source_format().audio,
        };
        let constraint = FormatConstraint {
            video: Some(VideoConstraint {
                codec: Some(vec![VideoCodec::H264]),
                ..VideoConstraint::default()
            }),
            ..FormatConstraint::default()
        };

        let mismatches = constraint.mismatches(&audio_only);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].field, "video");
        assert_eq!(mismatches[0].actual, "absent");
    }

    #[test]
    fn a_track_the_constraint_ignores_is_never_a_mismatch() {
        let constraint = FormatConstraint {
            audio: Some(AudioConstraint {
                sample_rate: Some(vec![48_000]),
                ..AudioConstraint::default()
            }),
            ..FormatConstraint::default()
        };
        assert!(
            constraint.satisfied_by(&source_format()),
            "video is present but unconstrained"
        );
    }

    #[test]
    fn framerate_keeps_drop_frame_distinct_from_its_rounding() {
        let mut ntsc = source_format();
        ntsc.video.as_mut().unwrap().framerate = Framerate::new(30_000, 1001);

        let constraint = FormatConstraint {
            video: Some(VideoConstraint {
                framerate: Some(vec![Framerate::new(30, 1)]),
                ..VideoConstraint::default()
            }),
            ..FormatConstraint::default()
        };

        let mismatches = constraint.mismatches(&ntsc);
        assert_eq!(mismatches.len(), 1, "29.97 is not 30");
        assert_eq!(mismatches[0].field, "video.framerate");
    }

    #[test]
    fn a_rejected_chroma_subsampling_is_reported_like_any_other_field() {
        let constraint = FormatConstraint {
            video: Some(VideoConstraint {
                chroma_subsampling: Some(vec![ChromaSubsampling::Yuv422]),
                ..VideoConstraint::default()
            }),
            ..FormatConstraint::default()
        };

        let mismatches = constraint.mismatches(&source_format());
        assert_eq!(mismatches.len(), 1);
        assert_eq!(
            mismatches[0].to_string(),
            "video.chroma_subsampling is yuv420 but accepts yuv422"
        );
    }

    #[test]
    fn format_and_constraint_round_trip_through_json() {
        let format = source_format();
        let round_trip: MediaFormat =
            serde_json::from_str(&serde_json::to_string(&format).unwrap()).unwrap();
        assert_eq!(format, round_trip);

        let constraint = FormatConstraint {
            audio: Some(AudioConstraint {
                sample_rate: Some(vec![44_100]),
                ..AudioConstraint::default()
            }),
            ..FormatConstraint::default()
        };
        let round_trip: FormatConstraint =
            serde_json::from_str(&serde_json::to_string(&constraint).unwrap()).unwrap();
        assert_eq!(constraint, round_trip);
    }

    #[test]
    fn a_misspelled_format_field_is_rejected() {
        let result: Result<MediaFormat, _> = serde_json::from_value(serde_json::json!({
            "container": "mpeg_ts",
            "audio": { "codec": "aac", "samplerate": 48000, "channels": 2 }
        }));
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }
}
