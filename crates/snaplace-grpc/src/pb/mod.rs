#[cfg(feature = "source")]
#[path = "./snaplace.grpc.request.rs"]
pub mod request;

#[cfg(feature = "sink")]
#[path = "./snaplace.grpc.response.rs"]
pub mod response;
