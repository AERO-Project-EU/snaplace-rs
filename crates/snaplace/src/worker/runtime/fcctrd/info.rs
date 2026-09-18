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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FcctrdFunctionInfo {
    pub id: FunctionId,

    #[serde(alias = "image")]
    pub image_ref: String,

    /// The memory required to run the Function [`Sandbox`].
    ///
    /// See the documentation of [`deserialize_byteunit`] for information on how this field
    /// is parsed.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`deserialize_byteunit`]: crate::utils::ser_de::deserialize_byteunit
    #[serde(deserialize_with = "crate::utils::ser_de::deserialize_byteunit")]
    pub memory: ByteUnit,

    pub parent_snapshot: Option<String>,

    /// Command to execute (like Docker's `entrypoint` and `cmd`, but already resolved/merged).
    ///
    /// # TODO
    ///
    /// This field can be populated either by the user that provides this struct, or by our system
    /// using the OCI Image spec's configuration.
    /// - The first case is the easiest to handle by our system: the `Runtime` just modifies the
    ///   `Spec` (e.g., through a template) accordingly.
    /// - The second case requires retrieving the OCI image spec, which requires a
    ///   firecracker-containerd client. At the moment, only `Runtime` has such a client. (This
    ///   is a runtime-dependent operation anyway). Given that, there are two cases:
    ///   1. Each `Runtime` can lazily populate `prcess_args` when it receives an empty (wrt that)
    ///      `FunctionInfo`, **right on the critical path** and **for every single request**
    ///      (since it cannot update the original `FunctionInfo` that lives in `SandboxPool`'s
    ///      store. This is bad.
    ///   2. The field could be populated at Function registration time, once and for all.
    ///      At the moment, we do not support Function registration though: the `Orchestrator` is
    ///      passed a statically pre-populated `FunctionMetadataStore` with the required
    ///      `FunctionInfo`.
    ///      It would make sense, however, to have a separate Registration Service that receives
    ///      `FunctionInfo` and populates the missing fields (e.g., `process_args`, but also
    ///      `parent_snapshot`).
    ///      To do that, for now, such a Registration Service would need a firecracker-containerd
    ///      client; in other words, it would be coupled with `Runtime` (possibly an associated
    ///      type?), which sounds reasonable anyway.
    ///      Nevertheless, such a Registration Service would also need write access to
    ///      `FunctionMetadataStore`, or to a separate store which would also be accessible (ro?)
    ///      by `Runtime` (hence `Worker`, hence `SandboxPool` too).
    ///
    /// # Notes
    ///
    /// - If none specified, the system will attempt to combine the related fields in the image's
    ///   OCI Image Configuration.
    ///
    /// - This is how containerd and cri-o merge `entrypoint` and `cmd`:
    ///   * <https://github.com/containerd/containerd/blob/v1.7.2/pkg/cri/opts/spec_opts.go#L58-L80>
    ///   * <https://github.com/cri-o/cri-o/blob/v1.27.0/internal/factory/container/container.go#L562-L598>
    pub process_args: Option<String>,
}

impl FunctionInfo for FcctrdFunctionInfo {
    #[inline(always)]
    fn id(&self) -> &FunctionId {
        &self.id
    }

    #[inline(always)]
    fn memory(&self) -> ByteUnit {
        self.memory
    }
}

impl TryFrom<RegisterFunctionRequest> for FcctrdFunctionInfo {
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

impl From<super::pb::FunctionInfo> for FcctrdFunctionInfo {
    #[inline]
    fn from(pbfi: super::pb::FunctionInfo) -> Self {
        Self {
            id: FunctionId::from(pbfi.function_id),
            image_ref: pbfi.image_ref,
            memory: pbfi.memory_mib.mebibytes(),
            parent_snapshot: pbfi.parent_snapshot,
            process_args: pbfi.process_args,
        }
    }
}

impl From<&FcctrdFunctionInfo> for super::pb::FunctionInfo {
    #[inline]
    fn from(fcfi: &FcctrdFunctionInfo) -> Self {
        Self {
            function_id: fcfi.id.as_str().into(),
            memory_mib: (fcfi.memory.as_u64() >> 20) as _,
            image_ref: fcfi.image_ref.clone(),
            parent_snapshot: fcfi.parent_snapshot.clone(),
            process_args: fcfi.process_args.clone(),
        }
    }
}
