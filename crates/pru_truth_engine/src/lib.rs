use anyhow::Result;
use pru_core::PruDbHandle;
use pru_media_schema::{
    get_detector_reliability, get_detector_scores_for_media, get_human_verdicts,
    DetectorReliability, MediaId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DetectionReport {
    pub probability_ai: f32,
    pub probability_human: f32,
    pub explanations: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TruthEngineConfig {
    pub default_detector_weight: f32,
    pub min_detectors_for_confident: usize,
}

impl Default for TruthEngineConfig {
    fn default() -> Self {
        Self {
            default_detector_weight: 1.0,
            min_detectors_for_confident: 1,
        }
    }
}

#[derive(Clone)]
pub struct TruthEngine {
    pub config: TruthEngineConfig,
}

impl TruthEngine {
    pub fn new(config: TruthEngineConfig) -> Self {
        Self { config }
    }

    pub fn evaluate_media(&self, pru: &PruDbHandle, media: MediaId) -> Result<DetectionReport> {
        let human_verdicts = get_human_verdicts(pru, media)?;
        if let Some(verdict) = human_verdicts.last() {
            let prob_ai = if verdict.eq_ignore_ascii_case("ai") {
                0.99
            } else {
                0.01
            };
            let prob_human = 1.0 - prob_ai;
            return Ok(DetectionReport {
                probability_ai: prob_ai,
                probability_human: prob_human,
                explanations: vec![format!("Human verdict present: {verdict}")],
            });
        }

        let detector_scores = get_detector_scores_for_media(pru, media)?;
        if detector_scores.is_empty() {
            return Ok(DetectionReport {
                probability_ai: 0.5,
                probability_human: 0.5,
                explanations: vec!["No detector scores found for this media".to_string()],
            });
        }

        let mut weighted_sum = 0.0_f32;
        let mut total_weight = 0.0_f32;
        let mut explanations = Vec::new();

        for (detector, score, label) in detector_scores {
            let reliability = get_detector_reliability(pru, detector)?;
            let weight = compute_weight(self.config.default_detector_weight, reliability);
            weighted_sum += (score as f32) * weight;
            total_weight += weight;
            explanations.push(format!(
                "Detector {}: score_ai={:.2}, label={}",
                detector.0, score, label
            ));
        }

        if total_weight == 0.0 {
            total_weight = 1.0;
        }
        let probability_ai = (weighted_sum / total_weight).clamp(0.0, 1.0);
        let probability_human = 1.0 - probability_ai;

        Ok(DetectionReport {
            probability_ai,
            probability_human,
            explanations,
        })
    }
}

fn compute_weight(default_weight: f32, reliability: Option<DetectorReliability>) -> f32 {
    if let Some(r) = reliability {
        let seen = r.seen as f32;
        let correct = r.correct as f32;
        default_weight * (correct + 1.0) / (seen + 2.0)
    } else {
        default_weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pru_core::PruStore;
    use pru_media_schema::{
        add_detector_score, add_human_verdict, ensure_detector_entity, upsert_media_entity,
        MediaType,
    };
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[test]
    fn human_verdict_overrides() {
        let dir = tempdir().unwrap();
        let store = PruStore::open(dir.path()).unwrap();
        let handle = Arc::new(Mutex::new(store));
        let media = upsert_media_entity(&handle, "hash", MediaType::Text).unwrap();
        add_human_verdict(&handle, media, "ai").unwrap();
        let engine = TruthEngine::new(TruthEngineConfig::default());
        let report = engine.evaluate_media(&handle, media).unwrap();
        assert!(report.probability_ai > 0.9);
    }

    #[test]
    fn detector_scores_aggregate() {
        let dir = tempdir().unwrap();
        let store = PruStore::open(dir.path()).unwrap();
        let handle = Arc::new(Mutex::new(store));
        let media = upsert_media_entity(&handle, "hash", MediaType::Text).unwrap();
        let detector = ensure_detector_entity(&handle, "detector:text:complexity_v1").unwrap();
        add_detector_score(&handle, media, detector, 0.8, "ai").unwrap();
        let engine = TruthEngine::new(TruthEngineConfig::default());
        let report = engine.evaluate_media(&handle, media).unwrap();
        assert!(report.probability_ai > 0.7);
    }
}
