use anyhow::{anyhow, Context, Result};
use exif;
use image::GenericImageView;
use pru_media_schema::MediaType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetectorMediaKind {
    Image,
    Text,
    Audio,
    Video,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetectorLabel {
    Ai,
    Human,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DetectorOutput {
    pub score_ai: f32,
    pub label: DetectorLabel,
    pub details: Option<String>,
}

pub trait MediaDetector: Send + Sync {
    fn id(&self) -> String;
    fn kind(&self) -> DetectorMediaKind;
    fn detect(&self, bytes: &[u8]) -> Result<DetectorOutput>;
}

#[derive(Default, Clone)]
pub struct DetectorRegistry {
    image_detectors: Vec<Arc<dyn MediaDetector>>,
    text_detectors: Vec<Arc<dyn MediaDetector>>,
    audio_detectors: Vec<Arc<dyn MediaDetector>>,
    video_detectors: Vec<Arc<dyn MediaDetector>>,
}

impl DetectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, detector: Arc<dyn MediaDetector>) {
        match detector.kind() {
            DetectorMediaKind::Image => self.image_detectors.push(detector),
            DetectorMediaKind::Text => self.text_detectors.push(detector),
            DetectorMediaKind::Audio => self.audio_detectors.push(detector),
            DetectorMediaKind::Video => self.video_detectors.push(detector),
        }
    }

    pub fn for_media(&self, kind: DetectorMediaKind) -> &[Arc<dyn MediaDetector>] {
        match kind {
            DetectorMediaKind::Image => &self.image_detectors,
            DetectorMediaKind::Text => &self.text_detectors,
            DetectorMediaKind::Audio => &self.audio_detectors,
            DetectorMediaKind::Video => &self.video_detectors,
        }
    }
}

pub struct TextComplexityDetector;

impl MediaDetector for TextComplexityDetector {
    fn id(&self) -> String {
        "detector:text:complexity_v1".to_string()
    }

    fn kind(&self) -> DetectorMediaKind {
        DetectorMediaKind::Text
    }

    fn detect(&self, bytes: &[u8]) -> Result<DetectorOutput> {
        let text = std::str::from_utf8(bytes).context("text must be utf-8")?;
        let words: Vec<&str> = text.split_whitespace().filter(|w| !w.is_empty()).collect();
        let total_chars: usize = words.iter().map(|w| w.chars().count()).sum();
        let avg_len = if words.is_empty() {
            0.0
        } else {
            total_chars as f32 / words.len() as f32
        };
        let unique: HashSet<&str> = words.iter().copied().collect();
        let vocab_ratio = if words.is_empty() {
            0.0
        } else {
            unique.len() as f32 / words.len() as f32
        };
        let repetition_score = 1.0 - vocab_ratio;
        let complexity_score = (avg_len / 10.0).clamp(0.0, 1.0);
        let ai_score = ((repetition_score * 0.6) + (1.0 - complexity_score) * 0.4).clamp(0.0, 1.0);
        let label = if ai_score > 0.55 {
            DetectorLabel::Ai
        } else {
            DetectorLabel::Human
        };
        Ok(DetectorOutput {
            score_ai: ai_score,
            label,
            details: Some(format!(
                "avg_len={avg_len:.2}, vocab_ratio={vocab_ratio:.2}, repetition={repetition_score:.2}"
            )),
        })
    }
}

pub struct ImageMetadataDetector;

impl MediaDetector for ImageMetadataDetector {
    fn id(&self) -> String {
        "detector:image:metadata_v1".to_string()
    }

    fn kind(&self) -> DetectorMediaKind {
        DetectorMediaKind::Image
    }

    fn detect(&self, bytes: &[u8]) -> Result<DetectorOutput> {
        // Try reading EXIF software tag.
        let mut ai_hint = 0.0_f32;
        let cursor = std::io::Cursor::new(bytes);
        if let Ok(exifreader) = exif::Reader::new().read_from_container(&mut cursor.clone()) {
            if let Some(field) = exifreader.get_field(exif::Tag::Software, exif::In::PRIMARY) {
                let soft = field.display_value().with_unit(&exifreader).to_string();
                let lower = soft.to_ascii_lowercase();
                if lower.contains("stable diffusion")
                    || lower.contains("dall-e")
                    || lower.contains("midjourney")
                {
                    ai_hint = 0.9;
                }
            }
        }

        let img = image::load_from_memory(bytes).map_err(|e| anyhow!("image decode: {e}"))?;
        let (w, h) = img.dimensions();
        let resolution = (w * h) as f32;
        let detail_score = ((resolution / 2_000_000.0).min(1.0)) * 0.3;
        let base_ai = (ai_hint + detail_score).clamp(0.0, 1.0);
        let label = if base_ai > 0.6 {
            DetectorLabel::Ai
        } else {
            DetectorLabel::Human
        };
        Ok(DetectorOutput {
            score_ai: base_ai,
            label,
            details: Some(format!("resolution={}x{}, ai_hint={ai_hint:.2}", w, h)),
        })
    }
}

pub fn media_type_to_kind(media_type: MediaType) -> DetectorMediaKind {
    match media_type {
        MediaType::Image => DetectorMediaKind::Image,
        MediaType::Text => DetectorMediaKind::Text,
        MediaType::Audio => DetectorMediaKind::Audio,
        MediaType::Video => DetectorMediaKind::Video,
    }
}
