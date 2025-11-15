use thiserror::Error;

#[derive(Debug, Error)]
pub enum PruError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON: {0}")]
    SerdeJson(#[from] serde_json::Error),

    // ⇩⇩ EKLE
    #[error("Persist: {0}")]
    Persist(#[from] tempfile::PersistError),

    #[error("Bad magic or version")]
    BadHeader,

    #[error("Corrupt record")]
    Corrupt,

    #[error("Unsupported kind")]
    Unsupported,

    #[error("Atom not found: {0}")]
    AtomNotFound(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, PruError>;
