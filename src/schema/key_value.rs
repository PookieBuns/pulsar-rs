use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    message::proto,
    schema::{schema_id_util, EncodeData, PulsarSchema},
    Error, Payload,
};

use super::schema_id_util::SchemaIdInfo;

/// Composes two inner schemas for key-value message types.
pub struct KeyValueSchema<K, V> {
    key_schema: Arc<dyn PulsarSchema<K>>,
    value_schema: Arc<dyn PulsarSchema<V>>,
}

impl<K, V> KeyValueSchema<K, V> {
    pub fn new(
        key_schema: Arc<dyn PulsarSchema<K>>,
        value_schema: Arc<dyn PulsarSchema<V>>,
    ) -> Self {
        Self {
            key_schema,
            value_schema,
        }
    }
}

/// Combine key and value payloads into a single byte vector.
/// Format: [4-byte key_len BE] [key_bytes] [value_bytes]
fn combine_kv_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut result = Vec::with_capacity(4 + key.len() + value.len());
    result.extend_from_slice(&key_len.to_be_bytes());
    result.extend_from_slice(key);
    result.extend_from_slice(value);
    result
}

/// Split a combined key-value payload into key and value Payloads.
fn split_kv_payload(payload: &Payload) -> Result<(Payload, Payload), Error> {
    if payload.data.len() < 4 {
        return Err(Error::Custom(
            "KeyValue payload too short to contain key length".to_string(),
        ));
    }
    let key_len =
        u32::from_be_bytes([payload.data[0], payload.data[1], payload.data[2], payload.data[3]])
            as usize;
    if payload.data.len() < 4 + key_len {
        return Err(Error::Custom(
            "KeyValue payload too short for declared key length".to_string(),
        ));
    }
    let key_data = payload.data[4..4 + key_len].to_vec();
    let val_data = payload.data[4 + key_len..].to_vec();
    Ok((
        Payload {
            metadata: payload.metadata.clone(),
            data: key_data,
        },
        Payload {
            metadata: payload.metadata.clone(),
            data: val_data,
        },
    ))
}

#[async_trait]
impl<K, V> PulsarSchema<(K, V)> for KeyValueSchema<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn schema_info(&self) -> proto::Schema {
        proto::Schema {
            r#type: proto::schema::Type::KeyValue as i32,
            ..Default::default()
        }
    }

    async fn encode(&self, topic: &str, message: (K, V)) -> Result<EncodeData, Error> {
        let (key, value) = message;
        let key_data = self.key_schema.encode(topic, key).await?;
        let value_data = self.value_schema.encode(topic, value).await?;
        let schema_id = schema_id_util::generate_kv_schema_id(
            key_data.schema_id.as_deref(),
            value_data.schema_id.as_deref(),
        );
        Ok(EncodeData {
            payload: combine_kv_payload(&key_data.payload, &value_data.payload),
            schema_id,
        })
    }

    async fn decode(
        &self,
        topic: &str,
        payload: &Payload,
        schema_id: Option<&[u8]>,
    ) -> Result<(K, V), Error> {
        let (key_id, value_id) = match schema_id {
            Some(data) => match schema_id_util::strip_magic_header(data)? {
                Some(SchemaIdInfo::KeyValue { key_id, value_id }) => {
                    (Some(key_id), Some(value_id))
                }
                _ => (None, None),
            },
            None => (None, None),
        };
        let (key_payload, value_payload) = split_kv_payload(payload)?;
        let key = self
            .key_schema
            .decode(topic, &key_payload, key_id.as_deref())
            .await?;
        let value = self
            .value_schema
            .decode(topic, &value_payload, value_id.as_deref())
            .await?;
        Ok((key, value))
    }

    async fn close(&self) -> Result<(), Error> {
        self.key_schema.close().await?;
        self.value_schema.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::schema_id_util;

    /// Simple mock schema for testing.
    struct MockSchema {
        schema_id: Option<Vec<u8>>,
    }

    #[async_trait]
    impl PulsarSchema<String> for MockSchema {
        fn schema_info(&self) -> proto::Schema {
            proto::Schema::default()
        }

        async fn encode(&self, _topic: &str, message: String) -> Result<EncodeData, Error> {
            Ok(EncodeData {
                payload: message.into_bytes(),
                schema_id: self.schema_id.clone(),
            })
        }

        async fn decode(
            &self,
            _topic: &str,
            payload: &Payload,
            _schema_id: Option<&[u8]>,
        ) -> Result<String, Error> {
            String::from_utf8(payload.data.clone()).map_err(|e| Error::Custom(e.to_string()))
        }
    }

    #[tokio::test]
    async fn test_kv_encode_decode_roundtrip() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema { schema_id: None }),
            Arc::new(MockSchema { schema_id: None }),
        );

        let encoded = kv_schema
            .encode("topic", ("key".to_string(), "value".to_string()))
            .await
            .unwrap();

        assert!(encoded.schema_id.is_none());

        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: encoded.payload,
        };
        let (k, v) = kv_schema.decode("topic", &payload, None).await.unwrap();
        assert_eq!(k, "key");
        assert_eq!(v, "value");
    }

    #[tokio::test]
    async fn test_kv_encode_with_schema_ids() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema {
                schema_id: Some(schema_id_util::add_magic_header(&[0x01])),
            }),
            Arc::new(MockSchema {
                schema_id: Some(schema_id_util::add_magic_header(&[0x02])),
            }),
        );

        let encoded = kv_schema
            .encode("topic", ("key".to_string(), "value".to_string()))
            .await
            .unwrap();

        let framed = encoded.schema_id.unwrap();
        assert_eq!(framed[0], schema_id_util::MAGIC_BYTE_KEY_VALUE);
    }

    #[tokio::test]
    async fn test_kv_schema_info() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema { schema_id: None }) as Arc<dyn PulsarSchema<String>>,
            Arc::new(MockSchema { schema_id: None }) as Arc<dyn PulsarSchema<String>>,
        );

        let info = kv_schema.schema_info();
        assert_eq!(info.r#type, proto::schema::Type::KeyValue as i32);
    }
}
