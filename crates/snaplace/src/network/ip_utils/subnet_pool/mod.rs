pub mod exact;
pub mod fifo;
pub mod fifo_exact;

#[derive(Debug, ::thiserror::Error)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum SubnetPoolError {
    #[error("use of wrong CIDR prefix")]
    PrefixLen(#[from] ::ipnet::PrefixLenError),
    #[error("requested IPv4 subnet '{0}' is not free")]
    SubnetNotFree(::ipnet::Ipv4Net),
}
