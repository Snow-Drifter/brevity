use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    TomlSer(#[from] toml::ser::Error),

    #[error(transparent)]
    TomlDe(#[from] toml::de::Error),

    #[error(transparent)]
    WinFsp(#[from] winfsp::FspError),

    #[error("path has no filename")]
    PathHasNoFileName,

    #[error("{} does not exist", .0.display())]
    PathNotFound(PathBuf),

    #[error("specify --add or --rm (or both)")]
    TagArgsEmpty,
}

impl From<windows::core::Error> for Error {
    fn from(value: windows::core::Error) -> Self {
        Self::WinFsp(value.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
