use std::{
    fmt::{self, Debug},
    marker::PhantomData,
};

use serde::{Deserialize, Serialize};

pub const CURRENT_VERSION: u64 = 0;
const TAG: u64 = 0xb6865496b901b888; // randomly generated

#[derive(Serialize, Deserialize)]
pub(crate) struct Metadata<T> {
    tag: MetadataTag,
    type_tag: TypeTag<T>,
    pub version: u64,
    pub stable_chunk_count: u64,
}

impl<T> Default for Metadata<T> {
    fn default() -> Self {
        Self {
            tag: MetadataTag,
            type_tag: TypeTag(PhantomData),
            version: CURRENT_VERSION,
            stable_chunk_count: 0,
        }
    }
}

impl<T> Clone for Metadata<T> {
    fn clone(&self) -> Self {
        Self {
            tag: MetadataTag,
            type_tag: TypeTag(PhantomData),
            version: self.version,
            stable_chunk_count: 0,
        }
    }
}

impl<T> Debug for Metadata<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Metadata")
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct MetadataTag;

impl Serialize for MetadataTag {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        TAG.serialize(_serializer)
    }
}

impl<'de> Deserialize<'de> for MetadataTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MetadataTagDeserializer;
        impl serde::de::Visitor<'_> for MetadataTagDeserializer {
            type Value = MetadataTag;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u64 with the HugeVec metadata tag")
            }

            fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let v = v
                    .try_into()
                    .map_err(|_| serde::de::Error::custom("metadata tag out of range"))?;
                self.visit_u64(v)
            }
            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(v as u64)
            }
            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(v as u64)
            }
            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(v as u64)
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value == TAG {
                    Ok(MetadataTag)
                } else {
                    Err(serde::de::Error::custom("Invalid metadata tag"))
                }
            }
        }

        deserializer.deserialize_u64(MetadataTagDeserializer)
    }
}

struct TypeTag<T>(PhantomData<T>);

impl<T> Serialize for TypeTag<T> {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // HACK: this may change between rustc versions, but should be stable enough for now
        let type_name = std::any::type_name::<T>();
        type_name.serialize(_serializer)
    }
}

impl<'de, T> Deserialize<'de> for TypeTag<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Default)]
        struct TypeTagDeserializer<T>(PhantomData<T>);

        impl<T> serde::de::Visitor<'_> for TypeTagDeserializer<T> {
            type Value = TypeTag<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string with the HugeVec's type name")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let type_name = std::any::type_name::<T>();
                if v == type_name {
                    Ok(TypeTag(PhantomData))
                } else {
                    Err(serde::de::Error::custom(format!(
                        "unexpected type name (wanted {type_name:?}, got {v:?}"
                    )))
                }
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match std::str::from_utf8(v) {
                    Ok(s) => self.visit_str(s),
                    Err(_) => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Bytes(v),
                        &self,
                    )),
                }
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match std::str::from_utf8(&v) {
                    Ok(s) => self.visit_str(s),
                    Err(_) => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Bytes(&v),
                        &self,
                    )),
                }
            }
        }

        deserializer.deserialize_str(TypeTagDeserializer(PhantomData))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn metadata_tag() {
        let serialized = bincode::serialize(&MetadataTag).unwrap();
        let _: MetadataTag = bincode::deserialize(&serialized).unwrap();
    }

    #[test]
    fn metadata_type_tag() {
        let type_tag: TypeTag<Vec<u8>> = TypeTag(PhantomData);
        let serialized = bincode::serialize(&type_tag).unwrap();
        let _: TypeTag<Vec<u8>> = bincode::deserialize(&serialized).unwrap();
    }
}
