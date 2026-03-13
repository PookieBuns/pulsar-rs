/// Magic byte for single-schema framing (PIP-420).
pub const MAGIC_BYTE_VALUE: u8 = 0xFF;
/// Magic byte for key-value schema framing (PIP-420).
pub const MAGIC_BYTE_KEY_VALUE: u8 = 0xFE;

/// Parsed schema ID information from magic-byte framed data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaIdInfo {
    Single(Vec<u8>),
    KeyValue { key_id: Vec<u8>, value_id: Vec<u8> },
}

pub fn add_magic_header(schema_id: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(1 + schema_id.len());
    result.push(MAGIC_BYTE_VALUE);
    result.extend_from_slice(schema_id);
    result
}

pub fn strip_magic_header(data: &[u8]) -> Option<SchemaIdInfo> {
    if data.is_empty() {
        return None;
    }
    match data[0] {
        MAGIC_BYTE_VALUE => Some(SchemaIdInfo::Single(data[1..].to_vec())),
        MAGIC_BYTE_KEY_VALUE => {
            if data.len() < 5 {
                return None;
            }
            let key_len =
                u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + key_len {
                return None;
            }
            let key_id = data[5..5 + key_len].to_vec();
            let value_id = data[5 + key_len..].to_vec();
            Some(SchemaIdInfo::KeyValue { key_id, value_id })
        }
        _ => None,
    }
}

pub fn generate_kv_schema_id(
    key_schema_id: Option<&[u8]>,
    value_schema_id: Option<&[u8]>,
) -> Option<Vec<u8>> {
    if key_schema_id.is_none() && value_schema_id.is_none() {
        return None;
    }
    let key_bytes = key_schema_id.unwrap_or(&[]);
    let val_bytes = value_schema_id.unwrap_or(&[]);
    let key_len = key_bytes.len() as u32;

    let mut result = Vec::with_capacity(1 + 4 + key_bytes.len() + val_bytes.len());
    result.push(MAGIC_BYTE_KEY_VALUE);
    result.extend_from_slice(&key_len.to_be_bytes());
    result.extend_from_slice(key_bytes);
    result.extend_from_slice(val_bytes);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_magic_header() {
        let id = vec![0x00, 0x00, 0x00, 0x01]; // schema ID = 1
        let framed = add_magic_header(&id);
        assert_eq!(framed[0], MAGIC_BYTE_VALUE);
        assert_eq!(&framed[1..], &id);
    }

    #[test]
    fn test_add_magic_header_empty() {
        let framed = add_magic_header(&[]);
        assert_eq!(framed, vec![MAGIC_BYTE_VALUE]);
    }

    #[test]
    fn test_strip_magic_header_single() {
        let id = vec![0x00, 0x00, 0x00, 0x05];
        let framed = add_magic_header(&id);
        let info = strip_magic_header(&framed).unwrap();
        assert_eq!(info, SchemaIdInfo::Single(id));
    }

    #[test]
    fn test_strip_magic_header_empty_input() {
        assert_eq!(strip_magic_header(&[]), None);
    }

    #[test]
    fn test_strip_magic_header_no_magic() {
        assert_eq!(strip_magic_header(&[0x00, 0x01, 0x02]), None);
    }

    #[test]
    fn test_generate_kv_schema_id_both() {
        let key_id = vec![0x00, 0x00, 0x00, 0x01];
        let val_id = vec![0x00, 0x00, 0x00, 0x02];
        let framed = generate_kv_schema_id(Some(&key_id), Some(&val_id)).unwrap();
        // [0xFE] [4-byte key_len BE] [key_id] [value_id]
        assert_eq!(framed[0], MAGIC_BYTE_KEY_VALUE);
        let key_len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(key_len, 4);
        assert_eq!(&framed[5..9], &key_id);
        assert_eq!(&framed[9..], &val_id);
    }

    #[test]
    fn test_generate_kv_schema_id_key_only() {
        let key_id = vec![0x00, 0x00, 0x00, 0x03];
        let framed = generate_kv_schema_id(Some(&key_id), None).unwrap();
        assert_eq!(framed[0], MAGIC_BYTE_KEY_VALUE);
        let key_len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(key_len, 4);
        assert_eq!(&framed[5..9], &key_id);
        assert!(framed[9..].is_empty());
    }

    #[test]
    fn test_generate_kv_schema_id_neither() {
        assert_eq!(generate_kv_schema_id(None, None), None);
    }

    #[test]
    fn test_roundtrip_kv() {
        let key_id = vec![0x01, 0x02];
        let val_id = vec![0x03, 0x04, 0x05];
        let framed = generate_kv_schema_id(Some(&key_id), Some(&val_id)).unwrap();
        let info = strip_magic_header(&framed).unwrap();
        assert_eq!(
            info,
            SchemaIdInfo::KeyValue {
                key_id: key_id,
                value_id: val_id,
            }
        );
    }
}
