pub mod plain_tap;
pub mod pool_tap;

use std::{fmt::Debug, future::Future};

use crate::network::Result;

pub trait Resource
where
    Self: Send + 'static + Debug,
{
    /// A description of this networking resource, possibly (de)serializable, that can be provided
    /// to [`SandboxNetworkingProvider::request`] (called internally through the `NetworkManager`),
    /// to reconstruct this specific `Resource` (i.e., with the same attributes, such as addresses,
    /// etc).
    type Descriptor: Send + 'static + Debug;
}

pub trait SandboxNetworkingProvider
where
    Self: Send + 'static,
{
    /// The [networking resource] associated with this `SandboxNetworkingProvider`.
    ///
    /// [networking resource]: Resource
    type Resource: Resource;

    /// Allocate and return a local [networking resource].
    ///
    /// [networking resource]: Resource
    fn alloc(&mut self) -> Result<Self::Resource>;

    /// Allocate a local [networking resource] with the specific characteristics specified in the
    /// provided [`Resource::Descriptor`].
    ///
    /// [networking resource]: Resource
    fn request(
        &mut self,
        desc: <Self::Resource as Resource>::Descriptor,
    ) -> impl Future<Output = Result<Self::Resource>> + Send;

    /// Deallocate the provided local [networking resource] (which was previously allocated via
    /// [`Self::alloc`]).
    ///
    /// [`Self::alloc`]: SandboxNetworkingProvider::alloc
    /// [networking resource]: Resource
    fn dealloc(&mut self, net_rsrc: Self::Resource) -> impl Future<Output = Result<()>> + Send;

    /// Shut down the provider, possibly cleaning up any resources no longer needed before exiting.
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send {
        ::futures::future::ok(())
    }
}

pub mod air_gapped {
    use crate::network::{self, SandboxNetworkingProvider};

    /// A no-op [`SandboxNetworkingProvider`] implementation.
    #[derive(Debug, Clone, Copy)]
    pub struct AirGapped;

    /// To be used in [`SandboxNetworkingProvider`] implementations that may not be
    /// associated with any resource at all.
    impl network::Resource for () {
        type Descriptor = ();
    }

    impl SandboxNetworkingProvider for AirGapped {
        type Resource = ();

        #[inline(always)]
        fn alloc(&mut self) -> network::Result<Self::Resource> {
            Ok(())
        }

        #[inline(always)]
        async fn request(
            &mut self,
            _: <Self::Resource as network::Resource>::Descriptor,
        ) -> network::Result<Self::Resource> {
            Ok(())
        }

        #[inline(always)]
        async fn dealloc(&mut self, _: Self::Resource) -> network::Result<()> {
            Ok(())
        }
    }
}
