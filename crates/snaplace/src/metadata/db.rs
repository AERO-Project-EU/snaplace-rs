use std::{any::type_name, fmt::Debug, marker::PhantomData};

use compact_str::CompactString;
use redb::{MultimapTableDefinition, TableDefinition, TypeName};
use serde::{Deserialize, Serialize};

use crate::{metadata::registration::RegisteredFunction, worker, FunctionId};

/// Auxiliary type to facilitate defining constant [`redb::TableDefinition`]s using generics.
#[allow(clippy::type_complexity)]
pub struct Tables<Rt>(PhantomData<fn() -> Rt>);

impl<Rt> Copy for Tables<Rt> {}

impl<Rt> Clone for Tables<Rt> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Rt> Debug for Tables<Rt> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "Tables<{}>", type_name::<Rt>(),)
    }
}

impl<Rt> Tables<Rt> {
    /// Database table containing generic (untyped) information about this snaplace deployment.
    pub const SNAPLACE: TableDefinition<'_, &str, &str> = TableDefinition::new("snaplace");
}

impl<Rt: worker::Runtime> Tables<Rt> {
    /// Database table that contains registered Functions, possibly registered during some
    /// past run.
    pub const FUNCTIONS: TableDefinition<
        '_,
        Rmp<FunctionId>,
        Rmp<RegisteredFunction<Rt::FunctionInfo>>,
    > = TableDefinition::new("snaplace::functions");

    /// Database table that contains snapshots of sandboxes, possibly created during some
    /// past run, each keyed by its Sandbox ID.
    pub const SNAPSHOTS: TableDefinition<
        '_,
        Rmp<CompactString>,
        Rmp<<Rt::Sandbox as worker::Sandbox>::SnapshotState>,
    > = TableDefinition::new("snaplace::snapshots");

    /// Database table that associates Functions with snapshots of sandboxes.
    /// Collections of Sandbox IDs are keyed by their associated [`FunctionId`].
    pub const SNAPS_PER_FUNC: MultimapTableDefinition<'_, Rmp<FunctionId>, Rmp<CompactString>> =
        MultimapTableDefinition::new("snaplace::snaps_per_func");
}

/// Wrapper type for serializing/deserializing into/from MessagePack using [`rmp_serde`] before
/// storing / after retrieving data to/from a [`redb::Database`].
#[derive(Debug)]
pub struct Rmp<T>(pub T);

impl<T> ::redb::Value for Rmp<T>
where
    T: Debug + Serialize + for<'de> Deserialize<'de>,
{
    type SelfType<'s>
        = T
    where
        Self: 's;
    type AsBytes<'s>
        = Vec<u8>
    where
        Self: 's;

    #[inline]
    fn type_name() -> redb::TypeName {
        TypeName::new(&format!("Rmp<{}>", ::std::any::type_name::<T>()))
    }

    #[inline]
    fn fixed_width() -> Option<usize> {
        None
    }

    #[inline]
    fn from_bytes<'s>(data: &'s [u8]) -> Self::SelfType<'s>
    where
        Self: 's,
    {
        ::rmp_serde::from_slice(data).expect("should receive valid MessagePack data?")
    }

    #[inline]
    fn as_bytes<'t, 's: 't>(value: &'t Self::SelfType<'s>) -> Self::AsBytes<'t>
    where
        Self: 's,
    {
        ::rmp_serde::to_vec(value).expect("should receive valid input Rmp<T>?")
    }
}

impl<T: Serialize + for<'de> Deserialize<'de> + Debug> ::redb::Key for Rmp<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> ::std::cmp::Ordering {
        data1.cmp(data2)
    }
}
