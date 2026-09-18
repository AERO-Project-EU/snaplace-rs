//! # Note
//!
//! In the future, we may extract this module from `snaplace` into a separate crate, to allow
//! using it independently (e.g., in some sort of agent running in sandboxes?).

mod ip;
mod link;
pub mod subnet_pool;

pub(crate) use ip::ip_addr_add;
pub(crate) use ip::set_link_address;
pub(crate) use ip::set_link_group;
pub(crate) use ip::set_link_up;
pub(crate) use link::Link;
