use anyhow::{Context, Result};
use pru_core::PruDbHandle;
use pru_detectors_api::{media_type_to_kind, DetectorRegistry};
use pru_media_schema::{
    add_content_hash, add_content_type, add_detector_score, hash_bytes, mark_analyzed_by,
    upsert_media_entity, MediaId, MediaType,
};

pub struct IngestResult {
    pub media_id: MediaId,
}

#[derive(Clone)]
pub struct IngestContext {
    pub pru: PruDbHandle,
    pub detectors: DetectorRegistry,
}

impl IngestContext {
    pub fn ingest_image(&self, bytes: &[u8]) -> Result<IngestResult> {
        self.ingest_generic(bytes, MediaType::Image)
    }

    pub fn ingest_text(&self, text: &str) -> Result<IngestResult> {
        self.ingest_generic(text.as_bytes(), MediaType::Text)
    }

    pub fn ingest_audio(&self, bytes: &[u8]) -> Result<IngestResult> {
        self.ingest_generic(bytes, MediaType::Audio)
    }

    pub fn ingest_video(&self, bytes: &[u8]) -> Result<IngestResult> {
        self.ingest_generic(bytes, MediaType::Video)
    }

    fn ingest_generic(&self, bytes: &[u8], media_type: MediaType) -> Result<IngestResult> {
        let hash = hash_bytes(bytes);
        let media_id = upsert_media_entity(&self.pru, &hash, media_type)?;
        add_content_type(&self.pru, media_id, media_type)?;
        add_content_hash(&self.pru, media_id, &hash)?;

        let kind = media_type_to_kind(media_type);
        for detector in self.detectors.for_media(kind).iter() {
            let output = detector.detect(bytes).with_context(|| detector.id())?;
            let detector_id = pru_media_schema::ensure_detector_entity(&self.pru, &detector.id())?;
            mark_analyzed_by(&self.pru, media_id, detector_id)?;
            add_detector_score(
                &self.pru,
                media_id,
                detector_id,
                output.score_ai as f64,
                &format!("{:?}", output.label),
            )?;
            if let Some(_details) = output.details.as_ref() {
                // placeholder
            }
        }

        Ok(IngestResult { media_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pru_core::PruStore;
    use pru_detectors_api::{ImageMetadataDetector, TextComplexityDetector};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[test]
    fn ingest_text_flow() {
        let dir = tempdir().unwrap();
        let store = PruStore::open(dir.path()).unwrap();
        let registry = {
            let mut r = DetectorRegistry::new();
            r.register(Arc::new(TextComplexityDetector));
            r
        };
        let ctx = IngestContext {
            pru: Arc::new(Mutex::new(store)),
            detectors: registry,
        };
        let result = ctx.ingest_text("hello world").unwrap();
        assert!(result.media_id.0 > 0);
    }

    #[test]
    fn ingest_image_flow() {
        let dir = tempdir().unwrap();
        let store = PruStore::open(dir.path()).unwrap();
        let registry = {
            let mut r = DetectorRegistry::new();
            r.register(Arc::new(ImageMetadataDetector));
            r
        };
        let ctx = IngestContext {
            pru: Arc::new(Mutex::new(store)),
            detectors: registry,
        };
        let img = image::RgbaImage::from_pixel(2, 2, image::Rgba([0, 0, 0, 0]));
        let mut buf = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut buf);
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut cursor, image::ImageFormat::Png)
            .unwrap();
        let result = ctx.ingest_image(&buf).unwrap();
        assert!(result.media_id.0 > 0);
    }
}
