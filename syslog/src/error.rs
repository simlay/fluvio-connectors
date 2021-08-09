use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Fluvio Error {0}")]
    Fluvio(#[from] fluvio::FluvioError),

    #[error("Io Error {0}")]
    Io(#[from] std::io::Error),

    #[error("Notify Error {0}")]
    Notify(#[from] notify::Error),

    #[error("Config Error {0}")]
    Config(#[from] ConfigError),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Io Error {0}")]
    IoError(#[from] std::io::Error),

    #[error("Toml Error {0}")]
    TomlError(#[from] toml::de::Error),
}
