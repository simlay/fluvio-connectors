
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio_controlplane_metadata::managed_connector::ManagedConnectorConfig;
use crate::error::ConnectorError;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    #[serde(rename = "type")]
    type_: String,
    topic: Option<String>,
    create_topic: Option<bool>,
    #[serde(default = "ConnectorConfig::default_args")]
    args: BTreeMap<String, String>,
}

pub type ConnectorConfigSet = BTreeMap<String, ConnectorConfig>;

impl ConnectorConfig {
    fn default_args() -> BTreeMap<String, String> {
        BTreeMap::new()
    }
    pub fn from_file<P: Into<PathBuf>> (path: P) -> Result<ConnectorConfigSet, ConnectorError> {

        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let connector_configs: ConnectorConfigSet = serde_yaml::from_str(&contents)?;
        Ok(connector_configs)

    }
}
impl Into<ManagedConnectorConfig> for ConnectorConfig {
    fn into(self) -> ManagedConnectorConfig {
        let topic = self.topic.unwrap_or(self.type_.clone());
        let args : Vec<String> = self.args.keys().zip(self.args.values()).flat_map(|(key, value)| [key.clone(), value.clone()]).collect::<Vec<_>>();
        ManagedConnectorConfig {
            type_: self.type_,
            topic,
            args,
        }
    }
}
