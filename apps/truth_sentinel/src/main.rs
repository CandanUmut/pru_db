use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Parser, Subcommand};
use pru_core::{PruDbHandle, PruStore};
use pru_detectors_api::{DetectorRegistry, ImageMetadataDetector, TextComplexityDetector};
use pru_ingest::IngestContext;
use pru_media_schema::{add_human_verdict, bump_reliability_from_verdict, MediaId};
use pru_truth_engine::{DetectionReport, TruthEngine, TruthEngineConfig};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

#[derive(Parser)]
#[command(author, version, about = "PRU Truth Engine CLI")]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Data directory for PRU store
    #[arg(long, default_value = "data/truth_sentinel")]
    data_dir: PathBuf,
}

#[derive(Subcommand)]
pub enum Commands {
    AnalyzeImage {
        path: PathBuf,
    },
    AnalyzeText {
        text: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    Label {
        media: String,
        label: String,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    fs::create_dir_all(&cli.data_dir)?;
    let store = PruStore::open(&cli.data_dir)?;
    let handle: PruDbHandle = Arc::new(Mutex::new(store));
    let registry = default_registry();
    let engine = TruthEngine::new(TruthEngineConfig::default());

    match cli.command {
        Commands::AnalyzeImage { path } => {
            let bytes = fs::read(&path)?;
            let ctx = IngestContext {
                pru: handle.clone(),
                detectors: registry.clone(),
            };
            let result = ctx.ingest_image(&bytes)?;
            let report = engine.evaluate_media(&handle, result.media_id)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report_with_id(result.media_id, report))?
            );
        }
        Commands::AnalyzeText { text, file } => {
            let content = if let Some(t) = text {
                t
            } else if let Some(file) = file {
                fs::read_to_string(file)?
            } else {
                use std::io::Read;
                let mut buffer = String::new();
                std::io::stdin().read_to_string(&mut buffer)?;
                buffer
            };
            let ctx = IngestContext {
                pru: handle.clone(),
                detectors: registry.clone(),
            };
            let result = ctx.ingest_text(&content)?;
            let report = engine.evaluate_media(&handle, result.media_id)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report_with_id(result.media_id, report))?
            );
        }
        Commands::Label { media, label } => {
            let media_id = resolve_media(&handle, &media)?;
            add_human_verdict(&handle, media_id, &label)?;
            bump_reliability_from_verdict(&handle, media_id, &label)?;
            println!("Labeled {media} as {label}");
        }
        Commands::Serve { addr } => {
            let state = AppState {
                handle: handle.clone(),
                registry: registry.clone(),
                engine,
            };
            let app = Router::new()
                .route("/analyze/text", post(analyze_text))
                .route("/analyze/image", post(analyze_image))
                .route("/label", post(label_media))
                .route("/media/:id/report", get(report_media))
                .layer(CorsLayer::permissive())
                .with_state(state);
            let listener = TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

fn default_registry() -> DetectorRegistry {
    let mut registry = DetectorRegistry::new();
    registry.register(Arc::new(TextComplexityDetector));
    registry.register(Arc::new(ImageMetadataDetector));
    registry
}

fn resolve_media(handle: &PruDbHandle, name: &str) -> Result<MediaId> {
    if let Ok(id) = name.parse::<u64>() {
        return Ok(MediaId(id));
    }
    let guard = handle.lock().unwrap();
    let entity = guard.get_entity_id(name).context("media not found")?;
    Ok(MediaId(entity))
}

fn report_with_id(id: MediaId, report: DetectionReport) -> serde_json::Value {
    serde_json::json!({
        "media_id": id.0,
        "probability_ai": report.probability_ai,
        "probability_human": report.probability_human,
        "explanations": report.explanations,
    })
}

#[derive(Clone)]
struct AppState {
    handle: PruDbHandle,
    registry: DetectorRegistry,
    engine: TruthEngine,
}

#[derive(Deserialize)]
struct TextRequest {
    text: String,
}

#[derive(Serialize)]
struct AnalyzeResponse {
    media_id: u64,
    probability_ai: f32,
    probability_human: f32,
    explanations: Vec<String>,
}

async fn analyze_text(
    State(state): State<AppState>,
    Json(body): Json<TextRequest>,
) -> Result<Json<AnalyzeResponse>, axum::http::StatusCode> {
    let ctx = IngestContext {
        pru: state.handle.clone(),
        detectors: state.registry.clone(),
    };
    let ingest = ctx
        .ingest_text(&body.text)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let report = state
        .engine
        .evaluate_media(&state.handle, ingest.media_id)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(AnalyzeResponse {
        media_id: ingest.media_id.0,
        probability_ai: report.probability_ai,
        probability_human: report.probability_human,
        explanations: report.explanations,
    }))
}

async fn analyze_image(
    State(state): State<AppState>,
    bytes: axum::body::Bytes,
) -> Result<Json<AnalyzeResponse>, axum::http::StatusCode> {
    let ctx = IngestContext {
        pru: state.handle.clone(),
        detectors: state.registry.clone(),
    };
    let ingest = ctx
        .ingest_image(&bytes)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let report = state
        .engine
        .evaluate_media(&state.handle, ingest.media_id)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(AnalyzeResponse {
        media_id: ingest.media_id.0,
        probability_ai: report.probability_ai,
        probability_human: report.probability_human,
        explanations: report.explanations,
    }))
}

#[derive(Deserialize)]
struct LabelRequest {
    media_id: String,
    label: String,
}

async fn label_media(
    State(state): State<AppState>,
    Json(body): Json<LabelRequest>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let media_id = resolve_media(&state.handle, &body.media_id)
        .map_err(|_| axum::http::StatusCode::BAD_REQUEST)?;
    add_human_verdict(&state.handle, media_id, &body.label)
        .and_then(|_| bump_reliability_from_verdict(&state.handle, media_id, &body.label))
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

async fn report_media(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let media_id =
        resolve_media(&state.handle, &id).map_err(|_| axum::http::StatusCode::BAD_REQUEST)?;
    let report = state
        .engine
        .evaluate_media(&state.handle, media_id)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(report_with_id(media_id, report)))
}
