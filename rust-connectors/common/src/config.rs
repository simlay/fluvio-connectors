use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use bytesize::ByteSize;

use fluvio::Compression;

#[derive(Debug, Clone, PartialEq, Default, Deserialize, Serialize)]
pub struct ConnectorConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,

    pub topic: String,
    pub version: String,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, ManagedConnectorParameterValue>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<String, SecretString>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerParameters>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerParameters>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ConsumerParameters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ProducerParameters {
    #[serde(with = "humantime_serde")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    linger: Option<Duration>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    compression: Option<Compression>,

    // This is needed because `ByteSize` serde deserializes as bytes. We need to use the parse
    // feature to populate `batch_size`.
    #[serde(rename = "batch-size")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    batch_size_string: Option<String>,

    #[serde(skip)]
    batch_size: Option<ByteSize>,
}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut connector_config: Self = serde_yaml::from_str(&contents)?;

        // This is needed because we want to use a human readable version of `BatchSize` but the
        // serde support for BatchSize serializes and deserializes as bytes.
        if let Some(ref mut producer) = &mut connector_config.producer {
            if let Some(batch_size_string) = &producer.batch_size_string {
                let batch_size = batch_size_string.parse::<ByteSize>()?;
                producer.batch_size = Some(batch_size);
            }
        }
        Ok(connector_config)
    }
}

#[test]
fn full_yaml_test() {
    //use pretty_assertions::assert_eq;
    let _connector_cfg = ConnectorConfig::from_file("test-data/connectors/full-config.yaml")
        .expect("Failed to load test config");
    /*
     * TODO; Check that this matches.
    let out: ManagedConnectorSpec = connector_cfg.into();
    let expected_params = BTreeMap::from([
        ("consumer-partition".to_string(), "10".to_string().into()),
        ("producer-linger".to_string(), "1ms".to_string().into()),
        (
            "producer-batch-size".to_string(),
            "44.0 MB".to_string().into(),
        ),
        (
            "producer-compression".to_string(),
            "gzip".to_string().into(),
        ),
        ("param_1".to_string(), "mqtt.hsl.fi".to_string().into()),
        (
            "param_2".to_string(),
            vec!["foo:baz".to_string(), "bar".to_string()].into(),
        ),
        (
            "param_3".to_string(),
            BTreeMap::from([
                ("bar".to_string(), "10.0".to_string()),
                ("foo".to_string(), "bar".to_string()),
                ("linger.ms".to_string(), "10".to_string()),
            ])
            .into(),
        ),
        ("param_4".to_string(), "true".to_string().into()),
        ("param_5".to_string(), "10".to_string().into()),
        (
            "param_6".to_string(),
            vec!["-10".to_string(), "-10.0".to_string()].into(),
        ),
    ]);
    assert_eq!(out.parameters, expected_params);
    */
}
#[test]
fn simple_yaml_test() {
    let _connector_cfg = ConnectorConfig::from_file("test-data/connectors/simple.yaml")
        .expect("Failed to load test config");
}

#[test]
fn error_yaml_tests() {
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-linger.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("Message(\"invalid value: string \\\"1\\\", expected a duration\", Some(Pos { marker: Marker { index: 118, line: 8, col: 10 }, path: \"producer.linger\" }))", format!("{:?}", connector_cfg));
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-compression.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("Message(\"unknown variant `gzipaoeu`, expected one of `none`, `gzip`, `snappy`, `lz4`\", Some(Pos { marker: Marker { index: 123, line: 8, col: 15 }, path: \"producer.compression\" }))", format!("{:?}", connector_cfg));

    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-batchsize.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!(
        "\"couldn't parse \\\"aoeu\\\" into a known SI unit, couldn't parse unit of \\\"aoeu\\\"\"",
        format!("{:?}", connector_cfg)
    );
    let connector_cfg = ConnectorConfig::from_file("test-data/connectors/error-version.yaml")
        .expect_err("This yaml should error");
    #[cfg(unix)]
    assert_eq!("Message(\"missing field `version`\", Some(Pos { marker: Marker { index: 4, line: 1, col: 4 }, path: \".\" }))", format!("{:?}", connector_cfg));
}

#[derive(Default, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]

/// Wrapper for string that does not reveal its internal
/// content in its display and debug implementation
pub struct SecretString(String);

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl FromStr for SecretString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl Deref for SecretString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(PartialEq, Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ManagedConnectorParameterValue {
    Vec(Vec<String>),
    Map(BTreeMap<String, String>),
    String(String),
}

impl Default for ManagedConnectorParameterValue {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;
struct ParameterValueVisitor;
impl<'de> Deserialize<'de> for ManagedConnectorParameterValue {
    fn deserialize<D>(deserializer: D) -> Result<ManagedConnectorParameterValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ParameterValueVisitor)
    }
}

impl<'de> Visitor<'de> for ParameterValueVisitor {
    type Value = ManagedConnectorParameterValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string, map or sequence")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str("null")
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str("null")
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(ManagedConnectorParameterValue::String(value.to_string()))
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut inner = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            inner.insert(key.clone(), value.clone());
        }

        Ok(ManagedConnectorParameterValue::Map(inner))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut vec_inner = Vec::new();
        while let Some(param) = seq.next_element::<String>()? {
            vec_inner.push(param);
        }
        Ok(ManagedConnectorParameterValue::Vec(vec_inner))
    }
}

#[test]
fn deserialize_test() {
    let yaml = r#"
name: kafka-out
parameters:
  param_1: "param_str"
  param_2:
   - item_1
   - item_2
   - 10
   - 10.0
   - true
   - On
   - Off
   - null
  param_3:
    arg1: val1
    arg2: 10
    arg3: -10
    arg4: false
    arg5: 1.0
    arg6: null
    arg7: On
    arg8: Off
  param_4: 10
  param_5: 10.0
  param_6: -10
  param_7: True
  param_8: 0xf1
  param_9: null
  param_10: 12.3015e+05
  param_11: [On, Off]
  param_12: true
secrets: {}
topic: poc1
type: kafka-sink
version: latest
"#;
    let connector_spec: ConnectorConfig =
        serde_yaml::from_str(yaml).expect("Failed to deserialize");
    println!("{:?}", connector_spec);
}
