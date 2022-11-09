use clap::Parser;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_connectors_common::{common_initialize, git_hash_version};
use fluvio_future::tracing::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schemars::schema_for;
use schemars::JsonSchema;
use std::str::FromStr;
use tokio_stream::StreamExt;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common_initialize!();
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "schema": schema_for!(KafkaOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let raw_opts = KafkaOpt::from_args();
    raw_opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Kafka sink connector",
    );

    let kafka_sink_deps = KafkaSinkDependencies::from_kafka_opt(raw_opts).await?;
    let kafka_producer = &kafka_sink_deps.kafka_producer;
    let kafka_partition = &kafka_sink_deps.kafka_partition;
    let kafka_topic = &kafka_sink_deps.kafka_topic;

    info!("Starting stream");

    let mut stream = kafka_sink_deps
        .common_connector_opt
        .create_consumer_stream("kafka")
        .await?;

    while let Some(Ok(record)) = stream.next().await {
        let mut kafka_record = FutureRecord::to(kafka_topic.as_str())
            .payload(record.value())
            .key(record.key().unwrap_or(&[]));
        if let Some(kafka_partition) = &(kafka_partition) {
            kafka_record = kafka_record.partition(*kafka_partition);
        }

        let enqueue_res = kafka_producer.send_result(kafka_record);

        if let Err((error, _)) = enqueue_res {
            error!(
                "KafkaError {:?}, offset: {}, partition: {}",
                error, &record.offset, &record.partition
            );
        }
    }

    Ok(())
}

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct KafkaOpt {
    /// A Comma separated list of the kafka brokers to connect to
    #[clap(long, env = "KAFKA_URL", hide_env_values = true)]
    pub kafka_url: String,

    #[clap(long)]
    pub kafka_topic: Option<String>,

    #[clap(long)]
    pub kafka_partition: Option<i32>,

    /// A key value pair in the form key:value
    #[clap(long)]
    pub kafka_option: Vec<String>,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub common: CommonConnectorOpt,

    #[clap(flatten)]
    #[schemars(flatten)]
    pub security: SecurityOpt,
}

pub struct KafkaSinkDependencies {
    pub kafka_topic: String,
    pub kafka_partition: Option<i32>,
    pub kafka_producer: FutureProducer,
    pub common_connector_opt: CommonConnectorOpt,
}

impl KafkaSinkDependencies {
    async fn from_kafka_opt(opts: KafkaOpt) -> anyhow::Result<KafkaSinkDependencies> {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::config::FromClientConfig;
        let KafkaOpt {
            kafka_url,
            kafka_topic,
            kafka_partition,
            kafka_option,
            common: common_connector_opt,
            security,
        } = opts;

        // Use fluvio_topic as kafka_topic fallback value
        let kafka_topic = kafka_topic.unwrap_or_else(|| common_connector_opt.fluvio_topic.clone());

        let client_config = {
            let mut kafka_options: Vec<(String, String)> = Vec::new();
            for opt in &kafka_option {
                let mut splits = opt.split(':');
                let (key, value) = (
                    splits.next().unwrap().to_string(),
                    splits.next().unwrap().to_string(),
                );
                kafka_options.push((key, value));
            }
            info!("kafka_options: {:?}", kafka_options);

            let mut client_config = ClientConfig::new();
            for (key, value) in &kafka_options {
                client_config.set(key, value);
            }
            client_config.set("bootstrap.servers", kafka_url.clone());
            if let Some(protocol) = security.security_protocol {
                client_config.set("security.protocol", protocol.to_string());
            }

            match (security.ssl_key_file, security.ssl_key_pem) {
                (Some(key_file), None) => {
                    if let Ok(key_contents) = std::fs::read_to_string(key_file) {
                        client_config.set("ssl.key.pem", key_contents);
                    }
                },
                (_, Some(key_pem)) => {
                    client_config.set("ssl.key.pem", key_pem);
                },
                (_, _) => {}
            }
            match (security.ssl_cert_file, security.ssl_cert_pem) {
                (Some(cert_file), None) => {
                    if let Ok(cert_contents) = std::fs::read_to_string(cert_file) {
                        client_config.set("ssl.certificate.pem", cert_contents);
                    }
                },
                (_, Some(cert_pem)) => {
                    client_config.set("ssl.certificate.pem", cert_pem);
                },
                (_, _) => {}
            }

            match (security.ssl_ca_file, security.ssl_ca_pem) {
                (Some(ca_file), None) => {
                    client_config.set("ssl.ca.location", ca_file);
                },
                (_, Some(ca_pem)) => {
                    let mut tmpfile = NamedTempFile::new().unwrap();
                    write!(tmpfile, "{}", ca_pem).unwrap();
                    let path = tmpfile.into_temp_path();
                    path.persist("/tmp/kafka-client-ca.pem")?;
                    client_config.set("ssl.ca.location", "/tmp/kafka-client-ca.pem");
                },
                (_, _) => {}
            }

            client_config
        };

        // Prepare topic, ensure it exists before creating producer
        let admin = AdminClient::from_config(&client_config)?;
        admin
            .create_topics(
                &[NewTopic::new(
                    kafka_topic.as_str(),
                    1,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await?;

        let kafka_producer = client_config.create().expect("Producer creation error");

        Ok(KafkaSinkDependencies {
            kafka_topic,
            kafka_partition,
            kafka_producer,
            common_connector_opt,
        })
    }
}

#[derive(Parser, Debug, JsonSchema, Clone)]
pub struct SecurityOpt {
    #[clap(long, group = "ssl-key")]
    pub ssl_key_file: Option<String>,

    #[clap(long, group = "ssl-key", env = "FLUVIO_KAFKA_CLIENT_KEY")]
    pub ssl_key_pem: Option<String>,

    #[clap(long, group = "ssl-cert")]
    pub ssl_cert_file: Option<String>,
    #[clap(long, group = "ssl-cert", env = "FLUVIO_KAFKA_CLIENT_CERT")]
    pub ssl_cert_pem: Option<String>,

    #[clap(long, group = "ssl-ca")]
    pub ssl_ca_file: Option<String>,
    #[clap(long, group = "ssl-ca", env = "FLUVIO_KAFKA_CLIENT_CA")]
    pub ssl_ca_pem: Option<String>,

    #[clap(long)]
    pub security_protocol: Option<SecurityProtocolOpt>,
}
#[derive(Parser, Debug, JsonSchema, Clone)]
pub enum SecurityProtocolOpt {
    SSL,
    // TODO: SASL_SSL and SASL_PLAINTEXT
}
impl ToString for SecurityProtocolOpt {
    fn to_string(&self) -> String {
        match self {
            Self::SSL => "SSL".to_string(),
        }
    }
}

impl FromStr for SecurityProtocolOpt {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.to_lowercase() == "ssl" {
            Ok(Self::SSL)
        } else {
            // TODO: Add SASL_SSL and SASL_PLAINTEXT
            Err("Invalid option. SSL is the only supported security protocol".into())
        }
    }
}
