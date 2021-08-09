use crate::error::ConfigError;
use serde::Deserialize;
use std::convert::TryFrom;
use std::path::Path;

use crate::error::ConnectorError;
use fluvio::{
    metadata::topic::{TopicReplicaParam, TopicSpec},
    Fluvio,
};
use fluvio_future::tracing;
use std::io;
use std::io::BufRead;

#[derive(Debug, Deserialize, Default)]
pub struct ConnectorConfig {
    pub(crate) source: Vec<SyslogSource>,
}

impl TryFrom<&Path> for ConnectorConfig {
    type Error = ConfigError;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        use std::fs::File;
        use std::io::Read;
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: ConnectorConfig = toml::from_str(&contents)?;
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub struct SyslogSource {
    name: String,
    r#type: String,
    filter_prefix: Option<String>,
    #[serde(default = "SyslogSource::default_topic")]
    topic: String,
    create_topic: Option<bool>,
    bind_url: Option<String>,
    input_file: Option<String>,
}

impl std::default::Default for SyslogSource {
    fn default() -> Self {
        Self {
            name: "syslog-connector".to_string(),
            r#type: "syslog".to_string(),
            filter_prefix: None,
            topic: "syslog".to_string(),
            create_topic: Some(true),
            bind_url: None,
            input_file: None,
        }
    }
}

impl SyslogSource {
    fn default_topic() -> String {
        "syslog".to_string()
    }
    pub async fn run(self) -> Result<(), ConnectorError> {
        let topic = &self.topic;
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;
        let topics = admin
            .list::<TopicSpec, _>(vec![])
            .await?
            .iter()
            .map(|topic| topic.name.clone())
            .collect::<String>();

        if !topics.contains(topic) {
            let _ = admin
                .create(
                    topic.clone(),
                    false,
                    TopicSpec::Computed(TopicReplicaParam::new(1, 1, false)),
                )
                .await?;
        }
        let producer = fluvio.topic_producer(&self.topic).await?;

        if let Some(ref _bind_url) = self.bind_url {
            todo!();
        } else if let Some(ref input_file) = self.input_file {
            use notify::{
                event::ModifyKind, EventKind, RecommendedWatcher, RecursiveMode,
                Result as NotifyResult, Watcher,
            };

            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher: RecommendedWatcher =
                RecommendedWatcher::new(move |res: NotifyResult<notify::Event>| match res {
                    Ok(event) => {
                        tracing::debug!("NEW EVENT: {:?}", event);
                        let _ = tx.send(event);
                    }
                    Err(e) => tracing::debug!("watch error: {:?}", e),
                })?;
            watcher.watch(Path::new(input_file), RecursiveMode::Recursive)?;

            let file = std::fs::File::open(input_file)?;
            let mut f = std::io::BufReader::new(file);
            tracing::debug!("Reading to the end of the file");

            // TODO: Figure out how to use SeekFrom here.
            loop {
                let mut line = String::new();
                let _ = f.read_line(&mut line);
                if line.is_empty() {
                    break;
                }
            }
            tracing::debug!("Now at the end of the file! Watching events");

            while let Ok(event) = rx.recv() {
                match event.kind {
                    EventKind::Modify(ModifyKind::Data(_)) => {
                        // TODO: There's certainly a better way to do this.
                        loop {
                            let mut line = String::new();
                            let _ = f.read_line(&mut line)?;
                            if line.is_empty() {
                                break;
                            }
                            if let Some(line) = line.strip_suffix('\n') {
                                let _ = producer.send("", line).await?;
                            }
                        }
                    }
                    other => {
                        tracing::debug!("OTHER EVENT {:?}", other);
                    }
                }
            }
        } else {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let line = line?;
                let _ = producer.send("", line).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod connector_tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;
    #[test]
    fn test_parsing() {
        let mut file = File::open("connector.toml").expect("Failed to open file");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Failed to read contents");
        let _config: ConnectorConfig = toml::from_str(&contents).expect("Failed to parse toml");
    }

    #[test]
    fn test_path() {
        let config = ConnectorConfig::try_from(Path::new("connector.toml"))
            .expect("Failed to get config from file");
        println!("CONFIG: {:#?}", config);
    }
}
