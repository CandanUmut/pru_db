use anyhow::Result;
use pru_core::{EntityId, PruDbHandle, PruStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub const PRED_HAS_HASH: &str = "has_hash";
pub const PRED_CONTENT_TYPE: &str = "content_type";
pub const PRED_ANALYZED_BY: &str = "analyzed_by";
pub const PRED_DETECTOR_SCORE: &str = "detector_score";
pub const PRED_DETECTOR_LABEL: &str = "detector_label";
pub const PRED_HAS_FEATURE: &str = "has_feature";
pub const PRED_PROVENANCE_CLAIM: &str = "provenance_claim";
pub const PRED_CAPTURED_BY_DEVICE: &str = "captured_by_device";
pub const PRED_CLAIMED_GENERATED_BY_MODEL: &str = "claimed_generated_by_model";
pub const PRED_SIMILAR_TO: &str = "similar_to";
pub const PRED_SEEN_ON: &str = "seen_on";
pub const PRED_HUMAN_VERDICT: &str = "human_verdict";
pub const PRED_DETECTOR_RELIABILITY: &str = "detector_reliability";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaType {
    Image,
    Text,
    Audio,
    Video,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MediaId(pub EntityId);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DetectorId(pub EntityId);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelFamilyId(pub EntityId);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub EntityId);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FeatureId(pub EntityId);

pub fn media_entity_name(hash: &str, media_type: MediaType) -> String {
    match media_type {
        MediaType::Image => format!("media:img:sha256:{hash}"),
        MediaType::Text => format!("media:txt:sha256:{hash}"),
        MediaType::Audio => format!("media:aud:sha256:{hash}"),
        MediaType::Video => format!("media:vid:sha256:{hash}"),
    }
}

pub fn detector_entity_name(id: &str) -> String {
    format!("detector:{id}")
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let hash = hasher.finalize();
    hex::encode(hash)
}

fn with_store<R>(handle: &PruDbHandle, f: impl FnOnce(&mut PruStore) -> Result<R>) -> Result<R> {
    let mut guard = handle.lock().expect("store poisoned");
    f(&mut guard)
}

pub fn upsert_media_entity(
    handle: &PruDbHandle,
    hash: &str,
    media_type: MediaType,
) -> Result<MediaId> {
    with_store(handle, |store| {
        let name = media_entity_name(hash, media_type);
        let id = store.intern_entity(&name)?;
        Ok(MediaId(id))
    })
}

pub fn add_content_type(handle: &PruDbHandle, media: MediaId, media_type: MediaType) -> Result<()> {
    with_store(handle, |store| {
        let pred = store.intern_predicate(PRED_CONTENT_TYPE)?;
        let lit = store.intern_literal(&format!("{:?}", media_type))?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: pred,
            object: lit,
            source: None,
            timestamp: None,
            confidence: None,
        })?;
        Ok(())
    })
}

pub fn add_detector_score(
    handle: &PruDbHandle,
    media: MediaId,
    detector: DetectorId,
    score: f64,
    label: &str,
) -> Result<()> {
    with_store(handle, |store| {
        let score_pred = store.intern_predicate(PRED_DETECTOR_SCORE)?;
        let label_pred = store.intern_predicate(PRED_DETECTOR_LABEL)?;
        let score_lit = store.intern_literal(&score.to_string())?;
        let label_lit = store.intern_literal(label)?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: score_pred,
            object: score_lit,
            source: Some(detector.0),
            timestamp: None,
            confidence: None,
        })?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: label_pred,
            object: label_lit,
            source: Some(detector.0),
            timestamp: None,
            confidence: None,
        })?;
        Ok(())
    })
}

pub fn mark_analyzed_by(handle: &PruDbHandle, media: MediaId, detector: DetectorId) -> Result<()> {
    with_store(handle, |store| {
        let pred = store.intern_predicate(PRED_ANALYZED_BY)?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: pred,
            object: detector.0,
            source: None,
            timestamp: None,
            confidence: None,
        })?;
        Ok(())
    })
}

pub fn add_human_verdict(handle: &PruDbHandle, media: MediaId, label: &str) -> Result<()> {
    with_store(handle, |store| {
        let pred = store.intern_predicate(PRED_HUMAN_VERDICT)?;
        let lit = store.intern_literal(label)?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: pred,
            object: lit,
            source: None,
            timestamp: None,
            confidence: Some(1.0),
        })?;
        Ok(())
    })
}

pub fn add_detector_reliability(
    handle: &PruDbHandle,
    detector: DetectorId,
    payload: &str,
) -> Result<()> {
    with_store(handle, |store| {
        let pred = store.intern_predicate(PRED_DETECTOR_RELIABILITY)?;
        let lit = store.intern_literal(payload)?;
        store.add_fact(pru_core::Fact {
            subject: detector.0,
            predicate: pred,
            object: lit,
            source: None,
            timestamp: None,
            confidence: None,
        })?;
        Ok(())
    })
}

pub fn get_detector_scores_for_media(
    handle: &PruDbHandle,
    media: MediaId,
) -> Result<Vec<(DetectorId, f64, String)>> {
    with_store(handle, |store| {
        let pred_score = match store.get_predicate_id(PRED_DETECTOR_SCORE) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let score_facts = store.facts_for_subject_predicate(media.0, pred_score)?;
        let mut results = Vec::new();
        for fact in score_facts {
            if let Some(src) = fact.source {
                if let Some(obj_str) = store.get_literal_value(fact.object) {
                    if let Ok(score) = obj_str.parse::<f64>() {
                        let label = find_label_for(store, media.0, src, PRED_DETECTOR_LABEL)?;
                        results.push((
                            DetectorId(src),
                            score,
                            label.unwrap_or_else(|| "unknown".into()),
                        ));
                    }
                }
            }
        }
        Ok(results)
    })
}

pub fn get_human_verdicts(handle: &PruDbHandle, media: MediaId) -> Result<Vec<String>> {
    with_store(handle, |store| {
        let pred = match store.get_predicate_id(PRED_HUMAN_VERDICT) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let facts = store.facts_for_subject_predicate(media.0, pred)?;
        Ok(facts
            .iter()
            .filter_map(|f| store.get_literal_value(f.object))
            .collect())
    })
}

fn find_label_for(
    store: &PruStore,
    media: EntityId,
    detector: EntityId,
    pred_name: &str,
) -> Result<Option<String>> {
    if let Some(pred) = store.get_predicate_id(pred_name) {
        let facts = store.facts_for_subject_predicate(media, pred)?;
        for fact in facts {
            if fact.source == Some(detector) {
                if let Some(val) = store.get_literal_value(fact.object) {
                    return Ok(Some(val));
                }
            }
        }
    }
    Ok(None)
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DetectorReliability {
    pub seen: u64,
    pub correct: u64,
}

pub fn get_detector_reliability(
    handle: &PruDbHandle,
    detector: DetectorId,
) -> Result<Option<DetectorReliability>> {
    with_store(handle, |store| {
        let Some(pred) = store.get_predicate_id(PRED_DETECTOR_RELIABILITY) else {
            return Ok(None);
        };
        let facts = store.facts_for_subject_predicate(detector.0, pred)?;
        for fact in facts.into_iter().rev() {
            if let Some(val) = store.get_literal_value(fact.object) {
                if let Ok(parsed) = serde_json::from_str::<DetectorReliability>(&val) {
                    return Ok(Some(parsed));
                }
            }
        }
        Ok(None)
    })
}

pub fn set_detector_reliability(
    handle: &PruDbHandle,
    detector: DetectorId,
    reliability: &DetectorReliability,
) -> Result<()> {
    let payload = serde_json::to_string(reliability)?;
    add_detector_reliability(handle, detector, &payload)
}

pub fn bump_reliability_from_verdict(
    handle: &PruDbHandle,
    media: MediaId,
    verdict_label: &str,
) -> Result<()> {
    let scores = get_detector_scores_for_media(handle, media)?;
    for (detector, _score, label) in scores {
        let mut reliability = get_detector_reliability(handle, detector)?.unwrap_or_default();
        reliability.seen += 1;
        if label.eq_ignore_ascii_case(verdict_label) {
            reliability.correct += 1;
        }
        set_detector_reliability(handle, detector, &reliability)?;
    }
    Ok(())
}

pub fn ensure_detector_entity(handle: &PruDbHandle, detector_name: &str) -> Result<DetectorId> {
    with_store(handle, |store| {
        let id = store.intern_entity(detector_name)?;
        Ok(DetectorId(id))
    })
}

pub fn add_content_hash(handle: &PruDbHandle, media: MediaId, hash: &str) -> Result<()> {
    with_store(handle, |store| {
        let pred = store.intern_predicate(PRED_HAS_HASH)?;
        let lit = store.intern_literal(hash)?;
        store.add_fact(pru_core::Fact {
            subject: media.0,
            predicate: pred,
            object: lit,
            source: None,
            timestamp: None,
            confidence: None,
        })?;
        Ok(())
    })
}

pub fn load_detector_labels(
    handle: &PruDbHandle,
    media: MediaId,
) -> Result<HashMap<EntityId, String>> {
    with_store(handle, |store| {
        let Some(pred) = store.get_predicate_id(PRED_DETECTOR_LABEL) else {
            return Ok(HashMap::new());
        };
        let facts = store.facts_for_subject_predicate(media.0, pred)?;
        let mut map = HashMap::new();
        for fact in facts {
            if let Some(src) = fact.source {
                if let Some(val) = store.get_literal_value(fact.object) {
                    map.insert(src, val);
                }
            }
        }
        Ok(map)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn upsert_media_creates_entity() {
        let dir = tempdir().unwrap();
        let store = PruStore::open(dir.path()).unwrap();
        let handle = std::sync::Arc::new(std::sync::Mutex::new(store));
        let media = upsert_media_entity(&handle, "abc", MediaType::Text).unwrap();
        assert!(media.0 > 0);
    }
}
