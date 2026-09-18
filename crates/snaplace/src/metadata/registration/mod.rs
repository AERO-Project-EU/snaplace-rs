use std::{fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};
use ubyte::ByteUnit;

use crate::{metadata::registration::pb::RegisterFunctionRequest, FunctionId};

pub mod pb;
mod registrar;
pub(crate) use registrar::Registrar;

/// The maximum memory that is allowed to be requested by a new Function.
pub const MAX_ALLOWED_FUNCTION_MEM: ByteUnit = ByteUnit::Gibibyte(10);

/// Implementors (which are [`Runtime`]-specific) provide sufficient information (to both their
/// [`Runtime`] _and_ the rest of the crate) about a Function.
///
/// [`Runtime`]: crate::worker::Runtime
pub trait FunctionInfo
where
    Self: TryFrom<RegisterFunctionRequest, Error: ::std::error::Error + Into<::tonic::Status>>
        + Serialize
        + for<'de> Deserialize<'de>
        + Clone
        + Debug
        + Send
        + Sync
        + 'static,
{
    /// Returns the [`FunctionId`] that uniquely identifies this Function.
    ///
    /// [`FunctionId`]: crate::FunctionId
    fn id(&self) -> &FunctionId;

    /// Returns the MiB of memory that need to be allocated to run a [`Sandbox`] of this Function.
    ///
    /// Note that this number represents MebiBytes (i.e., the actual number of
    /// required bytes will be calculated by multiplying this number by `2^20`).
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    fn memory(&self) -> ByteUnit;
}

/// Optional per-Function overrides for admission and dispatch policy.
///
/// Registered Functions may supply their own admission / queueing / scale-out
/// policy. Any field left as `None` falls back to the corresponding system-wide
/// default (see [`AdmissionConfig`]).
///
/// [`AdmissionConfig`]: crate::conf::AdmissionConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FunctionAdmissionOverrides {
    // The maximum number of requests that admission may queue for this Function.
    ///
    /// If `None`, defaults to [`AdmissionConfig::default_max_queued_per_func`].
    ///
    /// [`AdmissionConfig::default_max_queued_per_func`]:
    ///     crate::conf::AdmissionConfig::default_max_queued_per_func
    pub max_queued: Option<usize>,
    /// The maximum time a request for this Function may remain queued in
    /// admission before being failed.
    ///
    /// If `None`, defaults to [`AdmissionConfig::default_max_queue_delay`].
    ///
    /// [`AdmissionConfig::default_max_queue_delay`]:
    ///     crate::conf::AdmissionConfig::default_max_queue_delay
    #[serde(default, with = "humantime_serde::option")]
    pub max_queue_delay: Option<Duration>,
    /// Maximum number of live execution instances that may be kept for this
    /// Function at the same time.
    ///
    /// Idle and busy instances (i.e., _Idle_+_Active_ Workers) both count
    /// toward this limit.  Terminating instances (i.e., _Dying_ Workers) do not.
    ///
    /// If `None`, defaults to [`AdmissionConfig::default_max_live_instances_per_func`].
    ///
    /// [`AdmissionConfig::default_max_live_instances_per_func`]:
    ///     crate::conf::AdmissionConfig::default_max_live_instances_per_func
    pub max_live_instances: Option<usize>,
}

#[derive(Debug, ::thiserror::Error)]
pub enum AdmissionOverridesConversionError {
    #[error(transparent)]
    TryFromInt(#[from] ::std::num::TryFromIntError),
    #[error(transparent)]
    DurationError(#[from] ::prost_types::DurationError),
}

impl TryFrom<&pb::AdmissionOverrides> for FunctionAdmissionOverrides {
    type Error = AdmissionOverridesConversionError;

    fn try_from(ao: &pb::AdmissionOverrides) -> Result<Self, Self::Error> {
        Ok(Self {
            max_queued: ao.max_queued.map(TryInto::try_into).transpose()?,
            max_queue_delay: ao.max_queue_delay.map(TryInto::try_into).transpose()?,
            max_live_instances: ao.max_live_instances.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<&FunctionAdmissionOverrides> for pb::AdmissionOverrides {
    type Error = AdmissionOverridesConversionError;

    fn try_from(ao: &FunctionAdmissionOverrides) -> Result<Self, Self::Error> {
        Ok(pb::AdmissionOverrides {
            max_queued: ao.max_queued.map(TryInto::try_into).transpose()?,
            max_queue_delay: ao.max_queue_delay.map(TryInto::try_into).transpose()?,
            max_live_instances: ao.max_live_instances.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Complete registration record stored for one Function.
///
/// This combines the runtime-specific Function metadata with snaplace-owned
/// admission policy.  Both are immutable after registration and persisted
/// together so that start-up reinstatement always sees the original
/// registration state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RegisteredFunction<FnInfo> {
    /// Runtime-specific metadata used by the selected [`Runtime`].
    ///
    /// [`Runtime`]: crate::worker::Runtime
    info: FnInfo,
    /// Optional per-Function admission-policy overrides.
    ///
    /// `None` means all admission limits fall back to system-wide defaults.
    #[serde(default)]
    admission: Option<FunctionAdmissionOverrides>,
}

impl<FnInfo: FunctionInfo> RegisteredFunction<FnInfo> {
    #[inline]
    pub fn new(info: FnInfo, admission: Option<FunctionAdmissionOverrides>) -> Self {
        Self { info, admission }
    }

    /// Return the runtime-specific Function metadata.
    #[inline]
    pub fn info(&self) -> &FnInfo {
        &self.info
    }

    /// Return the per-Function admission overrides, if configured.
    #[inline]
    pub fn admission(&self) -> Option<&FunctionAdmissionOverrides> {
        self.admission.as_ref()
    }
}

#[derive(Debug, ::thiserror::Error)]
pub enum Error {
    //
    // Errors in deserialization
    //
    #[error("failed to deserialize protobuf")]
    DeserProto3(#[source] ::prost::DecodeError),
    #[error("failed to decode base64")]
    DecodeB64(#[source] ::base64::DecodeError),
    #[error("failed to deserialize JSON")]
    DeserJson(#[source] ::serde_json::Error),
    #[error("provided input missing field: '{0}'")]
    MissingField(Box<str>),
    //
    // Errors in FunctionMetadataStore
    //
    #[error("Function '{0}' not found in FunctionMetadataStore")]
    NotFound(FunctionId),
    #[error("Function '{0}' is already stored in FunctionMetadataStore")]
    AlreadyExists(FunctionId),
    #[error("Function '{fid}' is currently in use: {msg}")]
    InUse { fid: FunctionId, msg: Box<str> },
    //
    // Other errors
    //
    #[error("runtime failure during Function registration: {msg}")]
    Runtime {
        msg: Box<str>,
        #[source]
        err: Option<Box<dyn ::std::error::Error + Send + Sync + 'static>>, // TODO: use anyhow?
    },
    #[error("database error at Function registration: {msg}")]
    Database {
        msg: Box<str>,
        #[source]
        err: ::redb::Error,
    },
}

impl From<Error> for ::tonic::Status {
    fn from(err: Error) -> Self {
        use ::tonic::Status;
        match err {
            //
            // Errors in deserialization
            //
            Error::DeserProto3(err) => {
                Status::invalid_argument(format!("failed to deserialize protobuf: {err:#}"))
            }
            Error::DecodeB64(err) => {
                Status::invalid_argument(format!("failed to decode base64: {err:#}"))
            }
            Error::DeserJson(err) => {
                Status::invalid_argument(format!("failed to deserialize JSON: {err:#}"))
            }
            Error::MissingField(_) => Status::invalid_argument(err.to_string()),
            //
            // Errors in FunctionMetadataStore
            //
            Error::NotFound(_) => Status::not_found(format!("{err:#}")),
            Error::AlreadyExists(_) => Status::already_exists(format!("{err:#}")),
            Error::InUse { .. } => Status::failed_precondition(format!("{err:#}")),
            //
            // Other errors
            //
            Error::Runtime { msg, err } => Status::internal(format!(
                "runtime failure during Function registration: {msg}: (details: {err:?})"
            )),
            Error::Database { msg, err } => Status::data_loss(format!(
                "database failure at Function registration: {msg}: (details: {err:#})"
            )),
        }
    }
}
