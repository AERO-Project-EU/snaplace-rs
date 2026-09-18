mod error;
mod pb;
mod timestamping;
#[cfg(feature = "source")]
mod request;
#[cfg(feature = "sink")]
mod response;

pub use error::Error;

#[cfg(feature = "source")]
pub use pb::request::source_client::SourceClient;
#[cfg(feature = "source")]
pub use pb::request::Ack;
#[cfg(feature = "source")]
pub use pb::request::Request;
#[cfg(feature = "source")]
pub use request::Source;

#[cfg(feature = "sink")]
pub use pb::response::sink_client::SinkClient;
#[cfg(feature = "sink")]
pub use pb::response::Response;
#[cfg(feature = "sink")]
pub use pb::response::Subscribe;
#[cfg(feature = "sink")]
pub use response::Sink;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "source")]
    #[test]
    fn t01() {
        let map = [
            (String::from("a"), String::from("1")),
            (String::from("b"), String::from("2")),
        ]
        .into_iter()
        .collect();
        let payload = ::serde_json::to_vec(&map).unwrap().into();

        let _req = Request {
            invocation_id: String::from("test"),
            function_id: String::from("test"),
            metadata_map: map,
            payload,
        };
    }
}
