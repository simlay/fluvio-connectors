//!
//! # Create a Managed Connector
//!
//! CLI tree to generate Create a Managed Connector
//!

use fluvio_controlplane_metadata::managed_connector::ManagedConnectorSpec;
use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;

use crate::error::ConnectorError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateManagedConnectorOpt {
    /// The name for the new Managed Connector
    #[structopt(short = "c", long = "config", value_name = "config")]
    pub config: String,
}
use std::collections::BTreeMap;
use crate::config::ConnectorConfig;

impl CreateManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ConnectorError> {
        let configs = ConnectorConfig::from_file(&self.config)?;
        for (name, config) in configs.into_iter() {
            let spec = ManagedConnectorSpec {
                name: name.clone(),
                config: config.into()
            };

            debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

            let admin = fluvio.admin().await;
            admin.create(name.to_string(), false, spec).await?;
        }


        Ok(())
    }
}
