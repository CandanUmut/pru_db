use pru_core::PruStore;
use pru_detectors_api::{DetectorRegistry, TextComplexityDetector};
use pru_ingest::IngestContext;
use pru_truth_engine::{TruthEngine, TruthEngineConfig};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[test]
fn full_flow_text() {
    let dir = tempdir().unwrap();
    let store = PruStore::open(dir.path()).unwrap();
    let handle = Arc::new(Mutex::new(store));
    let mut registry = DetectorRegistry::new();
    registry.register(Arc::new(TextComplexityDetector));
    let ctx = IngestContext {
        pru: handle.clone(),
        detectors: registry,
    };
    let ingest = ctx.ingest_text("hello hello hello").unwrap();
    let engine = TruthEngine::new(TruthEngineConfig::default());
    let report = engine.evaluate_media(&handle, ingest.media_id).unwrap();
    assert!(report.probability_ai >= 0.0 && report.probability_ai <= 1.0);
}
