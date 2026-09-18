pub mod pb;

use std::time::Duration;

use tracing::{instrument, Level};

use crate::{
    control::sandbox::pb::{
        sandbox_lifecycle_server::SandboxLifecycle, CreateSnapshotRequest, CreateSnapshotResponse,
        DestroySandboxRequest, DestroySandboxResponse, PrepareSandboxRequest,
        PrepareSandboxResponse,
    },
    sbpool::{
        api::{
            DestroySandboxOptions, DestroySandboxRequestError, PrepareSandboxOptions,
            PrepareSandboxRequestError, SandboxControlState, SandboxProvisioningMode,
            SandboxSelector,
        },
        DestroySandboxError, PrepareSandboxError, SandboxPoolRef,
    },
};

pub(crate) struct SandboxLifecycleService {
    to_pool: SandboxPoolRef,
}

impl SandboxLifecycleService {
    pub fn new(to_pool: SandboxPoolRef) -> Self {
        Self { to_pool }
    }
}

#[::tonic::async_trait]
impl SandboxLifecycle for SandboxLifecycleService {
    #[instrument(
        level = Level::INFO,
        skip_all,
        fields(
            from = request.remote_addr().map_or_else(
                || "?".into(),
                |sa| format!("{sa}").into_boxed_str()
            ),
            function.id = request.get_ref().function_id.as_str(),
        )
    )]
    async fn prepare_sandbox(
        &self,
        request: ::tonic::Request<PrepareSandboxRequest>,
    ) -> Result<::tonic::Response<PrepareSandboxResponse>, ::tonic::Status> {
        let request = request.into_inner();
        let function_id = request.function_id.into();
        let options = PrepareSandboxOptions {
            mode: match pb::SandboxProvisioningMode::try_from(request.mode) {
                Ok(pb::SandboxProvisioningMode::PreferSnapshot) => {
                    SandboxProvisioningMode::PreferSnapshot
                }
                Ok(pb::SandboxProvisioningMode::RequireSnapshot) => {
                    SandboxProvisioningMode::RequireSnapshot
                }
                Ok(pb::SandboxProvisioningMode::ForceFresh) => SandboxProvisioningMode::ForceFresh,
                Ok(pb::SandboxProvisioningMode::Unspecified) | Err(_) => {
                    return Err(::tonic::Status::invalid_argument(format!(
                        "invalid provisioning mode: {}",
                        request.mode
                    )))
                }
            },
            initial_keepalive_hint: request
                .initial_keepalive_hint
                .map(Duration::try_from)
                .transpose()
                .map_err(|err| {
                    ::tonic::Status::invalid_argument(format!(
                        "invalid initial_keepalive_hint: {err}"
                    ))
                })?,
        };

        self.to_pool
            .prepare_sandbox(function_id, options)
            .await
            .map(|pso| {
                ::tonic::Response::new(PrepareSandboxResponse {
                    info: Some(pb::SandboxInfo {
                        sandbox_id: pso.info.sandbox_id.into(),
                        function_id: pso.info.function_id.to_string(),
                        state: match pso.info.state {
                            SandboxControlState::Active => pb::SandboxControlState::Active,
                            SandboxControlState::Idle => pb::SandboxControlState::Idle,
                            SandboxControlState::Dying => pb::SandboxControlState::Dying,
                            SandboxControlState::SnapshotOnly => {
                                pb::SandboxControlState::SnapshotOnly
                            }
                        } as _,
                        has_snapshot: pso.info.has_snapshot,
                        stats: pso.info.stats.map(|sb| pb::SandboxStats {
                            invocations: sb.invocations,
                            restorations: sb.restorations,
                        }),
                    }),
                })
            })
            .map_err(prepare_sandbox_status_from_err)
    }

    #[instrument(
        level = Level::INFO,
        skip_all,
        fields(
            from = request.remote_addr().map_or_else(
                || "?".into(),
                |sa| format!("{sa}").into_boxed_str()
            ),
            sandbox.id = request.get_ref().sandbox_id.as_str(),
        )
    )]
    async fn destroy_sandbox(
        &self,
        request: ::tonic::Request<DestroySandboxRequest>,
    ) -> Result<::tonic::Response<DestroySandboxResponse>, ::tonic::Status> {
        let request = request.into_inner();
        let sandbox_id = request.sandbox_id.into();

        let mut options = DestroySandboxOptions::default();
        if let Some(allow_if_active) = request.allow_if_active {
            options.allow_if_active = allow_if_active;
        }
        if let Some(remove_persisted_snapshot) = request.remove_persisted_snapshot {
            options.remove_persisted_snapshot = remove_persisted_snapshot;
        }

        self.to_pool
            .destroy_sandbox(SandboxSelector::SandboxId(sandbox_id), options)
            .await
            .map(|dso| {
                ::tonic::Response::new(DestroySandboxResponse {
                    persistent_snapshot_exists: dso.persistent_snapshot_exists,
                })
            })
            .map_err(destroy_sandbox_status_from_err)
    }

    #[instrument(
        level = Level::INFO,
        skip_all,
        fields(
            from = request.remote_addr().map_or_else(
                || "?".into(),
                |sa| format!("{sa}").into_boxed_str()
            ),
            sandbox.id = request.get_ref().sandbox_id.as_str(),
        )
    )]
    async fn create_snapshot(
        &self,
        request: ::tonic::Request<CreateSnapshotRequest>,
    ) -> Result<::tonic::Response<CreateSnapshotResponse>, ::tonic::Status> {
        let sandbox_id = request.into_inner().sandbox_id.into();
        self.to_pool
            .create_snapshot(sandbox_id)
            .await
            .map(|cso| {
                ::tonic::Response::new(CreateSnapshotResponse {
                    sandbox_id: cso.sandbox_id.into(),
                })
            })
            .map_err(create_snapshot_status_from_err)
    }
}

fn prepare_sandbox_status_from_err(err: PrepareSandboxRequestError) -> ::tonic::Status {
    match err {
        PrepareSandboxRequestError::Pool(err @ PrepareSandboxError::UnknownFunction(_)) => {
            ::tonic::Status::not_found(err.to_string())
        }
        PrepareSandboxRequestError::Pool(err @ PrepareSandboxError::OutOfSnapshots) => {
            ::tonic::Status::resource_exhausted(err.to_string())
        }
        PrepareSandboxRequestError::Pool(err @ PrepareSandboxError::OutOfMemory) => {
            ::tonic::Status::resource_exhausted(err.to_string())
        }
        PrepareSandboxRequestError::Pool(ref e @ PrepareSandboxError::Internal { ref err, .. }) => {
            ::tonic::Status::internal(format!("{e} (details? {err:?})"))
        }
        PrepareSandboxRequestError::Pool(err @ PrepareSandboxError::Worker(_)) => {
            ::tonic::Status::internal(err.to_string())
        }
        PrepareSandboxRequestError::SendTimeout => ::tonic::Status::unavailable(err.to_string()),
        PrepareSandboxRequestError::ReplyTimeout => {
            ::tonic::Status::deadline_exceeded(err.to_string())
        }
        PrepareSandboxRequestError::ChannelClosed { .. } => {
            ::tonic::Status::unavailable(err.to_string())
        }
    }
}

fn destroy_sandbox_status_from_err(
    err: crate::sbpool::api::DestroySandboxRequestError,
) -> ::tonic::Status {
    match err {
        DestroySandboxRequestError::Pool(err @ DestroySandboxError::NotFound(_)) => {
            ::tonic::Status::not_found(err.to_string())
        }
        DestroySandboxRequestError::Pool(err @ DestroySandboxError::Busy(_)) => {
            ::tonic::Status::unavailable(err.to_string())
        }
        DestroySandboxRequestError::Pool(ref e @ DestroySandboxError::Internal { ref err, .. }) => {
            ::tonic::Status::internal(format!("{e} (details? {err:?})"))
        }
        DestroySandboxRequestError::Pool(err @ DestroySandboxError::Worker(_)) => {
            ::tonic::Status::internal(err.to_string())
        }
        DestroySandboxRequestError::SendTimeout => ::tonic::Status::unavailable(err.to_string()),
        DestroySandboxRequestError::ReplyTimeout => {
            ::tonic::Status::deadline_exceeded(err.to_string())
        }
        DestroySandboxRequestError::ChannelClosed { .. } => {
            ::tonic::Status::unavailable(err.to_string())
        }
    }
}

fn create_snapshot_status_from_err(
    err: crate::sbpool::api::CreateSnapshotRequestError,
) -> ::tonic::Status {
    use crate::sbpool::{api::CreateSnapshotRequestError, CreateSnapshotError};
    match err {
        CreateSnapshotRequestError::Pool(err @ CreateSnapshotError::Disabled) => {
            ::tonic::Status::failed_precondition(err.to_string())
        }
        CreateSnapshotRequestError::Pool(err @ CreateSnapshotError::UnassignedSandboxId(_)) => {
            ::tonic::Status::not_found(err.to_string())
        }
        CreateSnapshotRequestError::Pool(err @ CreateSnapshotError::Busy(_)) => {
            ::tonic::Status::unavailable(err.to_string())
        }
        CreateSnapshotRequestError::Pool(err @ CreateSnapshotError::WorkerDying { .. }) => {
            ::tonic::Status::failed_precondition(err.to_string())
        }
        CreateSnapshotRequestError::Pool(ref e @ CreateSnapshotError::Internal { ref err, .. }) => {
            ::tonic::Status::internal(format!("{e} (details? {err:?})"))
        }
        CreateSnapshotRequestError::Pool(err @ CreateSnapshotError::SnapshotWorker(_)) => {
            ::tonic::Status::internal(err.to_string())
        }
        CreateSnapshotRequestError::SendTimeout => ::tonic::Status::unavailable(err.to_string()),
        CreateSnapshotRequestError::ReplyTimeout => {
            ::tonic::Status::deadline_exceeded(err.to_string())
        }
        CreateSnapshotRequestError::ChannelClosed { .. } => {
            ::tonic::Status::unavailable(err.to_string())
        }
    }
}
