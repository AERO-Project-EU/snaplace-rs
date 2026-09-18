pub mod backoff;
pub mod sd_static;

pub(crate) mod psutils;
#[cfg(feature = "uncache")]
pub(crate) mod uncache;

pub mod ser_de {
    use serde::{Deserialize, Deserializer};
    use ubyte::{ByteUnit, ToByteUnit};

    /// Values that are evaluated (by <code>([ByteUnit] as [FromStr])::[from_str]</code>)
    /// to be lesser than 1 MiB are multiplied by 1 MiB.
    ///
    /// For example:
    ///
    /// - `"512 MiB"` is deserialized as 512 MiB;
    /// - `"2097152"` is deserialized as 2 MiB;
    /// - `"100 GiB"` is deserialized as 100 GiB;
    ///
    /// but:
    ///
    /// - `"512"` is deserialized as 512 MiB;
    /// - `"2048"` is deserialized as 2 GiB;
    /// - `"4 KiB"` is deserialized as 4 GiB;
    /// - `"2048"` is deserialized as 2 GiB;
    ///
    /// [ByteUnit]: ubyte::ByteUnit
    /// [FromStr]: ::core::str::FromStr
    /// [from_str]: ::core::str::FromStr::from_str
    pub fn deserialize_byteunit<'de, D: Deserializer<'de>>(d: D) -> Result<ByteUnit, D::Error> {
        let b = ByteUnit::deserialize(d)?;
        Ok(if b > ByteUnit::MiB { b } else { b.mebibytes() })
    }
}
