use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use std::convert::TryFrom;
use std::io::{self, BufRead};
use std::path::Path;
use structopt::StructOpt;

use fluvio::{
    metadata::topic::{TopicReplicaParam, TopicSpec},
    Fluvio,
};

#[derive(StructOpt, Debug)]
pub struct ProducerOpts {
    #[structopt(short, long)]
    config: String,
}

impl ProducerOpts {
    pub async fn exec(self) -> Result<(), ConnectorError> {
        let config = ConnectorConfig::try_from(Path::new(&self.config))?;
        //let mut sources = Vec::new();
        for i in config.source {
            let _ = i.run().await;
            //sources.push(i.run());
        }
        //let foo = futures_util::future::join_all(sources).await;
        Ok(())
    }
}

struct SyslogProducer {
    config: ConnectorConfig,
}
