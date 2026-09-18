use enum_map_derive::Enum;
use serde::{Deserialize, Serialize};

/// Time measurements.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// The duration between the moments that:
    /// - the associated invocation [`Request`] was received by [`RequestSource`], and
    /// - [`AdmissionController`] acquired a [`RunningSlot`] for the [`Worker`]
    ///   assigned to the [`Request`].
    ///
    /// ## Note
    ///
    /// Contrary to the rest of the variants, this is calculated using the (non-monotonic) system
    /// clock, and is therefore susceptible to the related concerns (e.g., leaps, etc).
    ///
    ///
    /// [`AdmissionController`]: crate::admission::AdmissionController
    /// [`Request`]: crate::Request
    /// [`RequestSource`]: crate::request::Source
    /// [`RunningSlot`]: crate::admission::RunningSlot
    /// [`Worker`]: crate::worker::Worker
    Queued,
    /// Time spent in [`Worker`] while occupying one of the available running slots (to handle an
    /// invocation request).
    ///
    /// ## Note
    ///
    /// This contains both `((CreateSandbox & CreateSnapshot) | LoadSandbox | ResumeSandbox)` and
    /// `Issuer`.
    ///
    ///
    /// [`Worker`]: crate::worker::Worker
    RunningSlot,

    /// Time spent in [`Runtime`] to create a new [`Sandbox`], as perceived by the [`Worker`].
    ///
    /// ## Note
    ///
    /// This is disjoint with `LoadSandbox` and `ResumeSandbox`.
    ///
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::Sandbox
    /// [`Worker`]: crate::worker::Worker
    CreateSandbox,
    /// Time spent in [`Runtime`] to create a load a [`Sandbox`] from a
    /// snapshot, as perceived by the [`Worker`].
    ///
    /// ## Note
    ///
    /// This is disjoint with `CreateSandbox` and `ResumeSandbox`.
    ///
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::Sandbox
    /// [`Worker`]: crate::worker::Worker
    LoadSandbox,
    /// Time spent in [`Runtime`] to create a resume a paused [`Sandbox`], as
    /// perceived by the [`Worker`].
    ///
    /// ## Note
    ///
    /// This is disjoint with `CreateSandbox` and `LoadSandbox`.
    ///
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::Sandbox
    /// [`Worker`]: crate::worker::Worker
    ResumeSandbox,
    /// Time spent in [`Worker`] to setup any resources associated with an invocation, including
    /// time spent in its associated [`Runtime`].
    ///
    /// ## Note
    ///
    /// For now, this includes only:
    /// - CPU pinning (hence, it is present in every successful invocation),
    /// - [`Tap`] creation in cases a new `Sandbox` is created
    ///
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Tap`]: crate::network::Tap
    /// [`Worker`]: crate::worker::Worker
    SetupResources,
    /// Time spent in [`Runtime`] to create a [`Sandbox`] snapshot, as
    /// perceived by the [`Worker`].
    ///
    /// ## Note
    ///
    /// In current implementation, this should be always present along `CreateSandbox`.
    ///
    ///
    /// [`Runtime`]: crate::worker::Runtime
    /// [`Sandbox`]: crate::worker::Sandbox
    /// [`Worker`]: crate::worker::Worker
    CreateSnapshot,

    /// Time spent in the [`Worker`] waiting for the [`RequestIssuer`] to handle the invocation
    /// of the Function.
    ///
    /// ## Note
    ///
    /// This contains both `IssuerConnection` and `IssuerInvocation`.
    ///
    ///
    /// [`RequestIssuer`]: crate::worker::issuer::RequestIssuer
    /// [`Worker`]: crate::worker::Worker
    Issuer,
    /// Time spent in the [`RequestIssuer`] to `connect(2)` to [`Sandbox`]'s
    /// server.
    ///
    ///
    /// [`RequestIssuer`]: crate::worker::issuer::RequestIssuer
    /// [`Sandbox`]: crate::worker::Sandbox
    IssuerConnection,
    /// Time spent in the [`RequestIssuer`] to invoke [`Sandbox`]'s Function
    /// and retrieve the response.
    ///
    /// ## Note
    ///
    /// This contains `SandboxResponse`.
    ///
    ///
    /// [`RequestIssuer`]: crate::worker::issuer::RequestIssuer
    /// [`Sandbox`]: crate::worker::Sandbox
    IssuerInvocation,
    /// Time spent in the snaplace agent's handler within the [`Sandbox`] itself.
    ///
    /// ## Note
    ///
    /// This contains `SandboxHandler`.
    ///
    ///
    /// [`Sandbox`]: crate::worker::Sandbox
    SandboxResponse,
    /// Time spent in user's Function handler within the [`Sandbox`].
    ///
    ///
    /// [`Sandbox`]: crate::worker::Sandbox
    SandboxHandler,
}
