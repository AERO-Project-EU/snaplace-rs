use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use ubyte::{ByteUnit, ToByteUnit};

use crate::{
    metadata::{
        registration::{
            self,
            pb::{function_info::RuntimeSpecific, RegisterFunctionRequest},
        },
        FunctionInfo,
    },
    FunctionId,
};

// # Notes
//
// * __Rootfs path:__
//   It should not be part of the FunctionInfo. The latter is sent (over the network) by some
//   possibly remote agent, who might now know about our local filesystem paths.
//   The runtime should know where the rootfs is actually stored. E.g.:
//   - The rootfs could be created during Function registration, and placed at some path, while
//     the Runtime could cache that path for later use.
//   - `FunctionId`s could be employed for associating Functions with their rootfs.
//
/// Runtime-specific metadata required by the standalone Firecracker (`rt-fc`)
/// backend.
///
/// This describes how to size and boot a Function's microVM, but intentionally
/// does not include host-local rootfs paths. Rootfs placement is runtime-local
/// state derived during registration rather than data that should travel over
/// the wire with Function metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FcFunctionInfo {
    /// Unique identifier of the Function.
    pub id: FunctionId,

    /// The memory required to run the Function [`Sandbox`].
    ///
    /// See the documentation of [`deserialize_byteunit`] for information on how this field
    /// is parsed.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`deserialize_byteunit`]: crate::utils::ser_de::deserialize_byteunit
    #[serde(deserialize_with = "crate::utils::ser_de::deserialize_byteunit")]
    pub memory: ByteUnit,

    /// The number of vCPU cores to spawn [`Sandbox`]es of this Function with.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Runtime::Sandbox
    pub vcpu_count: u8,

    /// Guest command to execute for this Function.
    ///
    /// This defaults to the empty string during deserialization so callers may
    /// omit it when the guest image already provides its own startup behavior.
    ///
    /// # Note
    ///
    /// This is currently __unused__ by the `rt-fc` [`Runtime`] implementation.
    ///
    /// [`Runtime`]: crate::worker::Runtime
    #[serde(default)]
    pub entrypoint: String, // `Option<String>` to also allow `"entrypoint": null`->`""` ?
}

impl FunctionInfo for FcFunctionInfo {
    #[inline(always)]
    fn id(&self) -> &FunctionId {
        &self.id
    }

    #[inline(always)]
    fn memory(&self) -> ByteUnit {
        self.memory
    }
}

impl TryFrom<RegisterFunctionRequest> for FcFunctionInfo {
    type Error = registration::Error;

    fn try_from(req: RegisterFunctionRequest) -> Result<Self, Self::Error> {
        match req.function_info {
            Some(fi) => match fi.runtime_specific {
                Some(RuntimeSpecific::Proto3(any)) => any
                    .to_msg::<super::pb::FunctionInfo>()
                    .map(Into::into)
                    .map_err(registration::Error::DeserProto3),
                Some(RuntimeSpecific::B64Json(base64_bytes)) => BASE64
                    .decode(base64_bytes)
                    .map_err(registration::Error::DecodeB64)
                    .and_then(|json_bytes| {
                        ::serde_json::from_slice(json_bytes.as_ref())
                            .map_err(registration::Error::DeserJson)
                    }),
                None => Err(registration::Error::MissingField(
                    "RegisterFunctionRequest.function_info.runtime_specific".into(),
                )),
            },
            None => Err(registration::Error::MissingField(
                "RegisterFunctionRequest.function_info".into(),
            )),
        }
    }
}

impl From<super::pb::FunctionInfo> for FcFunctionInfo {
    #[inline]
    fn from(pbfi: super::pb::FunctionInfo) -> Self {
        Self {
            id: FunctionId::from(pbfi.function_id),
            memory: pbfi.memory_mib.mebibytes(),
            vcpu_count: pbfi.vcpu_count as _,
            entrypoint: pbfi.entrypoint,
        }
    }
}

impl From<&FcFunctionInfo> for super::pb::FunctionInfo {
    #[inline]
    fn from(fcfi: &FcFunctionInfo) -> Self {
        Self {
            function_id: fcfi.id.as_str().into(),
            memory_mib: (fcfi.memory.as_u64() >> 20) as _,
            vcpu_count: fcfi.vcpu_count as _,
            entrypoint: fcfi.entrypoint.clone(),
        }
    }
}
