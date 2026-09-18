//! Utilities to support the management of TAP devices in `snaplace`.
//!
//! <div class="warning">
//!
//! This is not a generic library/module to manage TUN/TAP.
//!
//! It was originally meant to support the specific needs of the [`PlainTapDevices`] networking
//! provider in `snaplace`.
//!
//! </div>
//!
//! > ##### Note (ckatsak):
//! >
//! > The reason this is not included in [`plain_tap`] is that I believe that it might be useful in
//! > other [`SandboxNetworkingProvider`]s as well, especially ones who might be pretty similar to
//! > [`PlainTapDevices`] (e.g., plain TAPs, but pooled).
//!
//!
//! [`plain_tap`]: crate::network::providers::plain_tap
//! [`PlainTapDevices`]: crate::network::providers::plain_tap::PlainTapDevices
//! [`SandboxNetworkingProvider`]: crate::network::providers::SandboxNetworkingProvider

use std::{ffi::CStr, fs::OpenOptions, mem::MaybeUninit, net::Ipv4Addr, slice};

use compact_str::{CompactString, ToCompactString};
use mac_address::MacAddress;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

#[cfg(feature = "rt-fcctrd")]
use firecracker_containerd_client::{FirecrackerNetworkInterface, NetworkInterfaceBuilder};

use crate::network::{
    self,
    ip_utils::{ip_addr_add, set_link_address, set_link_group, set_link_up, Link},
    Error,
};

mod ioctl {
    use linux_raw_sys::{ctypes::c_int, net::ifreq};
    use rustix::{
        fd::AsFd,
        io::Result,
        ioctl::{ioctl, opcode, Setter, Updater},
    };

    pub const TUNDEV: &str = "/dev/net/tun";

    /// # References
    ///
    /// - <https://github.com/shemminger/iproute2/blob/v6.1.0/ip/iptuntap.c#L119>
    /// - <https://github.com/vishvananda/netlink/blob/v1.2.1-beta.2/link_linux.go#L28>
    ///
    /// - <https://ldpreload.com/p/tuntap-notes.txt>
    pub(crate) const TAP_FLAGS: i32 = ::libc::IFF_NO_PI
        | ::libc::IFF_VNET_HDR
        | ::libc::IFF_ONE_QUEUE
        | ::libc::IFF_TUN_EXCL
        | ::libc::IFF_TAP;

    /// Create a new tun/tap device.
    ///
    /// This corresponds to Linux `tuntap` driver's `TUNSETIFF` (`_IOW`) `ioctl(2)`.
    ///
    /// # References
    ///
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/Documentation/userspace-api/ioctl/ioctl-number.rst#L195>
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/include/uapi/linux/if_tun.h#L34>
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/drivers/net/tun.c#L3085>
    /// - <https://github.com/shemminger/iproute2/blob/v6.1.0/ip/iptuntap.c#L67>
    /// - `if_tun.h`
    #[inline]
    pub fn tun_set_iff<F: AsFd>(fd: F, mut ifr: ifreq) -> Result<()> {
        unsafe {
            let ctl = Updater::<{ opcode::write::<c_int>(b'T', 202) }, _>::new(&mut ifr);
            ioctl(fd, ctl)
        }
    }

    /// Allows the tun/tap device to continue to exist even when the last file descriptor has
    /// been closed.
    ///
    /// This corresponds to Linux `tuntap` driver's `TUNSETPERSIST` (`_IOW`) `ioctl(2)`.
    ///
    /// # References
    ///
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/Documentation/userspace-api/ioctl/ioctl-number.rst#L195>
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/include/uapi/linux/if_tun.h#L35>
    /// - <https://elixir.bootlin.com/linux/v6.2.1/source/drivers/net/tun.c#L3144>
    /// - <https://github.com/shemminger/iproute2/blob/v6.1.0/ip/iptuntap.c#L79>
    #[inline]
    pub fn tun_set_persist<F: AsFd>(fd: F, persist: bool) -> Result<()> {
        unsafe {
            let ctl = Setter::<{ opcode::write::<c_int>(b'T', 203) }, c_int>::new(persist.into());
            ioctl(fd, ctl)
        }
    }
}

/// # Safety
///
/// The provided [`ifreq`] must be valid and correctly initialized (e.g., one returned by
/// [`prepare_ifreq`]).
///
/// [`ifreq`]: ::linux_raw_sys::net::ifreq
#[inline]
unsafe fn adjust_name(ifr: &::linux_raw_sys::net::ifreq, old_len: usize) -> CompactString {
    // Allocate a temp `&[u8]` with length == `IFNAMSIZ` for `ifr.ifr_ifrn.ifrn_name`.
    // SAFETY: Invariants are held as long as `ifr.ifr_ifrn.ifrn_name` is a valid, initialized
    // `[i8; IFNAMSIZ]`.
    let name = unsafe {
        slice::from_raw_parts(
            ifr.ifr_ifrn.ifrn_name.as_ptr() as _,
            ::linux_raw_sys::net::IFNAMSIZ as _,
        )
    };

    // Cast the `&[u8]` to `&CStr`.
    // SAFETY: `name` is nul-terminated and has no interior nul bytes -- although it may
    // have multiple trailing nul bytes at the end.
    let name = unsafe { CStr::from_bytes_with_nul_unchecked(name) };
    // Alternatively (though probably not better really):
    //let name = CStr::from_bytes_until_nul(name).expect("tap name is NUL-terminated");

    // Allocate a new `CompactString` on the stack.
    // SAFETY: As long as `ifreq`'s invariants hold (i.e., it is correctly initialized), the
    // tap's name is valid ASCII, hence valid UTF-8 too.
    let mut name = CompactString::new(name.to_str().expect("tap name is valid UTF-8"));

    // Ignore trailing nul bytes in the `CompactString` by manually modifying its length.
    let new_len = old_len.min(::linux_raw_sys::net::IFNAMSIZ as usize - 1);
    // SAFETY:
    // - `name`'s length should be <= 16, since it originates from `ifr.ifr_ifrn.ifrn_name`;
    //   therefore, its capacity should be 24 (being a stack-allocated `CompactString`).
    // - `new_len` is guaranteed to be at most `IFNAMSIZ - 1` (== 15 < 24) bytes long, but also
    //   never longer than the given `name` (i.e., `old_len`); therefore, `new_len` bytes are
    //   always initialized properly.
    unsafe { name.set_len(new_len) };

    debug_assert!(!name.is_heap_allocated(), "tap name should live in stack");
    name
}

#[inline]
fn prepare_ifreq(name: &str) -> ::linux_raw_sys::net::ifreq {
    let ifr = MaybeUninit::<::linux_raw_sys::net::ifreq>::zeroed();
    // SAFETY: There are neither references nor function pointers in `::linux_raw_sys::net::ifreq`,
    // hence `0` is a valid bit pattern and there is no undefined behavior.
    let mut ifr = unsafe { ifr.assume_init() };

    // We aim to copy at most `IFNAMSIZ - 1` into ifrn_name, so that its 16th byte remains '\0'.
    let strlen = name.len().min(::linux_raw_sys::net::IFNAMSIZ as usize - 1);
    // SAFETY:
    // - both src and dst are valid pointers to the heap and the stack, respectively;
    // - we copy at most `IFNAMSIZ - 1` bytes into `ifreq->ifr_name`;
    // - if we copy fewer than `IFNAMSIZ - 1`, the rest of the bytes in `ifr_name` are '\0'.
    // - `i8`s in `if.ifr_name` are positive, hence no problem expected by casting them to `u8`s
    unsafe {
        let src = name.as_ptr();
        let dst = &mut ifr.ifr_ifrn.ifrn_name as *mut _ as _;
        ::core::ptr::copy_nonoverlapping(src, dst, strlen)
    };

    // SAFETY:
    // - `ifr->ifr_ifru.ifru_flags` is the only field accessed throughout this union instance's
    //   lifetime;
    // - `0xF002 == TAP_FLAGS > i16::MAX`, because `IFF_TUN_EXCL == 0x8000 > i16::MAX` but
    //   this (UB?) is how things are defined in the kernel and followed by iproute2 as well:
    //   * https://elixir.bootlin.com/linux/v6.2.1/source/include/uapi/linux/if.h#L247
    //   * https://elixir.bootlin.com/linux/v6.2.1/source/include/uapi/linux/if_tun.h#L76
    //   * https://github.com/shemminger/iproute2/blob/v6.1.0/ip/iptuntap.c#L59
    //   so we just do the same.
    unsafe { ifr.ifr_ifru.ifru_flags |= ioctl::TAP_FLAGS as i16 };

    ifr
}

/// Do the actual `ioctl(2)` calls to the underlying tuntap driver to create the tap device.
///
/// This should be equivalent to `iproute2`'s command:
///
/// ```bash
/// # ip tuntap add dev "$name" mode tap
/// ```
///
/// # Safety
///
/// `ifreq` must be initialized correctly (see SAFETY notes in `prepare_ifreq`).
#[inline]
unsafe fn create_tap_device(ifr: ::linux_raw_sys::net::ifreq) -> Result<(), Error> {
    let tunf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(ioctl::TUNDEV)
        .map_err(|err| {
            error!(error = ?err, "Error opening '{}': {err:#}", ioctl::TUNDEV);
            Error::TapInit(Box::new(Error::Io {
                msg: format!("error opening '{}'", ioctl::TUNDEV).into_boxed_str(),
                source: err,
            }))
        })?;

    ioctl::tun_set_iff(&tunf, ifr).map_err(Error::TunSetIff)?;
    ioctl::tun_set_persist(&tunf, true).map_err(Error::TunSetPersist)?;

    Ok(())
}

#[inline]
async fn delete_tap(rtnl: &::rtnetlink::Handle, index: u32, name: &str) -> Result<(), Error> {
    rtnl.link().del(index).execute().await.map_err(|err| {
        error!(error = ?err, "Error deleting tap device '{name}': {err:#}");
        Error::RouteNetlink(err)
    })
}

/// Create a new persistent tap device with the given `name`.
///
/// The `name` is assumed to be valid for use as an interface's name (i.e., it is not checked
/// or validated in any way other than keeping its first `IFNAMSIZ - 1` bytes).
///
/// # Panics
///
/// The function may panic if the provided `name` for the tap device is not valid for use as
/// the name of a Linux network interface.
async fn new_tap(
    rtnl: &::rtnetlink::Handle,
    name: CompactString,
    ip_addr: Ipv4Addr,
    prefix_len: u8,
    gateway: Ipv4Addr,
    mac_addr: Option<MacAddress>,
) -> Result<TapDevice, Error> {
    // Spawn the ioctl(2) calls on a separate thread to avoid blocking the runtime
    // NOTE(ckatsak): Keep `::linux_raw_sys::ifreq` in a nested scope because it is `!Send`
    // (due to its internal union's field `ifr_ifru.ifru_data`, whose type is a `mut` pointer,
    // `*mut ::linux_raw_sys::ctypes::c_void` -- `mut` pointer types are always `!Send`), which
    // causes a compilation error at subsequent `.await` points (e.g., the next one is ~19 lines
    // below: the `Link::by_name()` call).
    let name = ::tokio::task::spawn_blocking(move || {
        let ifr = prepare_ifreq(name.as_str());

        // SAFETY: tunf is safely created and ifreq has just been initialized correctly
        unsafe { create_tap_device(ifr) }.map_err(|err| {
            warn!(error = ?err, "Error creating new tap device: {err:#}");
            Error::TapInit(Box::new(err))
        })?;

        // SAFETY: `ifr` has just been initialized correctly
        Ok::<_, Error>(unsafe { adjust_name(&ifr, name.len()) })
    })
    .await
    .map_err(Error::JoinTask)??;

    // Retrieve link information from rtnetlink
    let mut link = Link::by_name(rtnl, name.as_str()).await?;

    // Get the MAC address for the new TAP, also setting it if needed
    let mac_addr = if let Some(address) = mac_addr {
        // If a MAC address has been provided, then set the new TAP's address to that
        // # ip link set dev "$link.name" address "$address"
        match set_link_address(rtnl, &mut link, &address).await {
            Ok(()) => Ok(address),
            Err(err) => {
                error!(
                    error = ?err,
                    "Error setting the provided MAC address '{address}' to link '{}': {err:#}",
                    link.index()
                );
                match delete_tap(rtnl, link.index(), &name).await {
                    Ok(()) => warn!("Deleted tap '{name}' after failing to set its MAC address"),
                    Err(err) => error!(
                        error = ?err,
                        "Failed to delete tap '{name}' after failing to set its MAC address: {err:#}"
                    ),
                }
                Err(Error::TapInit(Box::new(err)))
            }
        }
    } else {
        // If no MAC address has been provided, look up the one assigned by the kernel
        match link.mac_addr() {
            Ok(addr) => Ok(addr),
            Err(err) => {
                error!(error = ?err, "Error looking up MAC address for link '{name}': {err:#}");
                match delete_tap(rtnl, link.index(), &name).await {
                    Ok(()) => {
                        warn!("Deleted tap '{name}' after failing to retrieve its MAC address")
                    }
                    Err(err) => error!(
                        error = ?err,
                        "Failed to delete tap '{name}' after failing to retrieve its MAC address: {err:#}"
                    ),
                }
                Err(Error::TapInit(Box::new(err)))
            }
        }
    }?;

    // # ip addr add "$gateway/$prefix_len" dev "$link.name"
    // On the host's side, the tap is assigned the `gateway` IP address; `ip_addr` is passed to
    // the guest kernel (as cmd line arg) to be assigned to the virtual iface *inside* the VM.
    if let Err(err) = ip_addr_add(rtnl, &mut link, gateway, prefix_len).await {
        error!(error = ?err, "Failed to add IP address to link '{name}': {err:#}");
        match delete_tap(rtnl, link.index(), &name).await {
            Ok(()) => warn!("Deleted tap '{name}' after failing to add IP address"),
            Err(err) => error!(
                error = ?err,
                "Failed to delete tap '{name}' after failing to add IP address: {err:#}",
            ),
        }
        return Err(Error::TapInit(Box::new(err)));
    }

    // # ip link set dev "$link.name" group "$DEVGROUP"
    if let Err(err) = set_link_group(rtnl, &mut link).await {
        error!(error = ?err, "Failed to set the group of link '{name}': {err:#}");
        match delete_tap(rtnl, link.index(), &name).await {
            Ok(()) => warn!("Deleted tap '{name}' after failing to set its group"),
            Err(err) => error!(
                error = ?err,
                "Failed to delete tap '{name}' after failing to set its group: {err:#}",
            ),
        }
        return Err(Error::TapInit(Box::new(err)));
    }

    // # ip link set "$link.name" up
    if let Err(err) = set_link_up(rtnl, &mut link).await {
        error!(error = ?err, "Failed to set link '{name}' UP: {err:#}");
        match delete_tap(rtnl, link.index(), &name).await {
            Ok(()) => warn!("Deleted tap '{name}' after failing to set it UP"),
            Err(err) => error!(
                error = ?err,
                "Failed to delete tap '{name}' after failing to set it UP: {err:#}",
            ),
        }
        return Err(Error::TapInit(Box::new(err)));
    }

    Ok(TapDevice {
        index: link.index(),
        info: TapInfo {
            name,
            mac_addr: Some(mac_addr),
            ip_addr,
            prefix_len,
            gateway,
        },
    })
}

/// Information about a [`Tap`], regardless of its current state (i.e., of whether it has been
/// created yet or not).
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TapInfo {
    /// The name of the TAP device on the host.
    pub name: CompactString,
    /// The MAC (L2) address of the TAP device on the host, assigned by the kernel (or by
    /// `systemd`, if the `MACAddressPolicy` has not been configured to `none`, which is not
    /// recommended).
    pub mac_addr: Option<MacAddress>,

    /// The IP address assigned to the `virtio-net` device assocated with this TAP interface.
    pub ip_addr: Ipv4Addr,
    /// The prefix length of the IP address (i.e., the length of the network part of its IP
    /// address).
    pub prefix_len: u8,
    /// The IP address assigned to this TAP interface, which presumably acts as the default gateway
    /// for the VM attached to the associated `virtio-net` device.
    pub gateway: Ipv4Addr,
}

impl ::std::fmt::Debug for TapInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        const UNKNOWN_MAC_ADDR: CompactString = CompactString::const_new("?");
        let mac_addr = self
            .mac_addr
            .as_ref()
            .map(ToCompactString::to_compact_string)
            .unwrap_or(UNKNOWN_MAC_ADDR);

        f.debug_struct("TapInfo")
            .field("name", &self.name)
            .field("mac_addr", &mac_addr)
            .field(
                "ip_addr",
                &format_args!("{}/{}", self.ip_addr, self.prefix_len),
            )
            .field("gateway", &self.gateway)
            .finish()
    }
}

impl ::std::fmt::Display for TapInfo {
    #[inline]
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::std::fmt::Debug::fmt(self, f)
    }
}

/// A TAP device that has been created and initialized, and includes an interfrace index.
#[derive(Debug, Serialize, Deserialize)]
pub struct TapDevice {
    index: u32,
    info: TapInfo,
}

impl ::std::fmt::Display for TapDevice {
    #[inline]
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::std::fmt::Debug::fmt(self, f)
    }
}

impl TapDevice {
    /// Create a new persistent tap device with the given `name`.
    ///
    /// The `name` is assumed to be valid for use as an interface's name (i.e., it is not checked
    /// or validated in any way other than keeping its first `IFNAMSIZ - 1` bytes).
    ///
    /// This should be equivalent to `iproute2`'s command:
    ///
    /// ```bash
    /// # ip tuntap add dev "$name" mode tap
    /// ```
    ///
    /// # Panics
    ///
    /// The function may panic if the provided `name` for the tap device is not valid for use as
    /// the name of a Linux network interface.
    #[allow(dead_code)]
    #[inline]
    pub(super) async fn new(
        rtnl: &::rtnetlink::Handle,
        name: CompactString,
        ip_addr: Ipv4Addr,
        prefix_len: u8,
        gateway: Ipv4Addr,
        mac_addr: Option<MacAddress>,
    ) -> Result<Self, Error> {
        new_tap(rtnl, name, ip_addr, prefix_len, gateway, mac_addr).await
    }

    #[inline]
    pub(crate) async fn destroy(self, rtnl: &::rtnetlink::Handle) -> Result<(), Error> {
        delete_tap(rtnl, self.index, &self.info.name).await
    }

    #[inline]
    pub fn info(&self) -> &TapInfo {
        &self.info
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.info.name
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }

    #[inline]
    pub fn mac_addr(&self) -> Option<MacAddress> {
        self.info.mac_addr
    }

    #[inline]
    pub fn ip_addr(&self) -> Ipv4Addr {
        self.info.ip_addr
    }

    #[inline]
    pub fn prefix_len(&self) -> u8 {
        self.info.prefix_len
    }

    #[inline]
    pub fn gateway(&self) -> Ipv4Addr {
        self.info.gateway
    }

    /// Based on the attributes of this `TapDevice`, construct a new
    /// [`FirecrackerNetworkInterface`].
    ///
    /// # Notes
    ///
    /// The newly constructed [`FirecrackerNetworkInterface`] returned has its MAC address
    /// unset (i.e., `""`). This is valid, since the corresponding `guest_mac` field in
    /// [Firecracker's API][fc-yaml-guestmac] is _optional_. Firecracker will generate
    /// a valid MAC address for its new `virtio-net` device (in conformance with
    /// [virtio-v1.2 spec, §5.1.4.2][virtio1.2-5.1.4.2]).
    ///
    /// ## Past
    ///
    /// In the past, to build a [`FirecrackerNetworkInterface`] via this method, the `TapDevice`
    /// used to provide its own MAC address to [`NetworkInterfaceBuilder`]. This effectively leads
    /// to both the underlying TAP device _and_ the VM's `virtio-net` device having ___the same___
    /// MAC address.
    ///
    /// In general, this is a bad idea: two devices in the same L2 broadcast domain having the
    /// same L2 address can lead to FDB flapping and generally __packet loss__ for both of them.
    ///
    /// ### Why?
    ///
    /// For instance, say `vm1` and `vm2` each has its own `virtio-net` device, each connected to
    /// its own TAP device, `tap1` and `tap2`, which are both connected to the same Linux bridge,
    /// `br0`, and:
    /// - `tap1.dev_addr == vm1(virtio-net).MAC == M1`,
    /// - `tap2.dev_addr == vm2(virtio-net).MAC == M2`.
    ///
    /// When `tap1` and `tap2` are enslaved to `br0`, the Linux bridge automatically installs
    /// **local FDB entries** for each port’s own device MAC (its `dev_addr`). Those “local”
    /// entries are special: frames whose destination MAC matches a local FDB entry are
    /// _terminated locally_ at the bridge and are _not forwarded_ (see [`bridge(8)`][man8bridge]).
    ///
    /// Therefore, any frame that arrives at `br0` with `dst=M1` (or `dst=M2`) will be __consumed__
    /// by the host (the bridge device), __not forwarded__ to `tap1` (`tap2`). That blackholes
    /// unicast traffic destined to the VM. (Mind that this is particularly difficult to debug...)
    ///
    /// ### Then why did it use to work?
    ///
    /// In the special case where the entire L2 segment consists of only two nodes (e.g., in the
    /// case of [`PlainTapDevices`], with `/31` subnets), there is no bridge/switch learning
    /// MAC<->port mappings. The entire segment is effectively merely a point-to-point L2 link,
    /// and the L2 address "uniqueness" is not enforced by anything. In this specific case, the
    /// two devices (TAP and `virtio-net`) having the same MAC address just happens to work; it
    /// does not create problems, albeit it is "unconventional".
    ///
    ///
    /// [`PlainTapDevices`]: crate::network::providers::plain_tap::PlainTapDevices
    /// [fc-yaml-guestmac]: https://github.com/firecracker-microvm/firecracker/blob/v1.13.1/src/firecracker/swagger/firecracker.yaml#L1148-L1149
    /// [man8bridge]: https://man7.org/linux/man-pages/man8/bridge.8.html
    /// [virtio1.2-5.1.4.2]: https://docs.oasis-open.org/virtio/virtio/v1.2/cs01/virtio-v1.2-cs01.html#x1-2250002
    #[inline]
    #[cfg(feature = "rt-fcctrd")]
    pub fn to_firecracker_if(
        &self,
        nameservers: impl IntoIterator<Item = impl Into<Ipv4Addr>>,
    ) -> FirecrackerNetworkInterface {
        NetworkInterfaceBuilder::new(
            self.info.name.as_str(),
            self.info.ip_addr,
            self.info.prefix_len,
            self.info.gateway,
        )
        //.guest_mac_addr(self.info.mac_addr) // see Notes above
        .nameservers(nameservers)
        .build()
    }
}

impl network::Resource for TapDevice {
    type Descriptor = TapInfo;
}

/// A TAP device that has **NOT** been created and initialized yet, and includes a
/// [`rtnetlink::Handle`] to do that later.
#[derive(Debug, Clone)]
pub struct TapBuilder {
    rtnl: ::rtnetlink::Handle,
    info: TapInfo,
}

impl TapBuilder {
    #[inline]
    pub(crate) fn new(rtnl: ::rtnetlink::Handle, info: TapInfo) -> Self {
        Self { rtnl, info }
    }

    /// Attempt to create and initialize a new [`TapDevice`], consuming this [`TapBuilder`].
    ///
    /// # Errors
    ///
    /// An [`Error::TapBuild`] containing the source [`Error`] is returned in case of failure.
    #[inline]
    pub(crate) async fn build(self) -> Result<TapDevice, Error> {
        new_tap(
            &self.rtnl,
            self.info.name,
            self.info.ip_addr,
            self.info.prefix_len,
            self.info.gateway,
            self.info.mac_addr,
        )
        .await
        .map_err(|err| Error::TapBuild {
            ip_addr: self.info.ip_addr,
            prefix_len: self.info.prefix_len,
            source: Box::new(err),
        })
    }

    /// Similarly to [`TapBuilder::build`], attempt to create and initialize a new [`TapDevice`],
    /// consuming this [`TapBuilder`].
    /// However, this method additionally handles [`EBUSY`] returned by [`ioctl(TUNSETIFF)`] by
    /// querying `rtnetlink` for the TAP device's index to return a corresponding [`TapDevice`].
    ///
    /// # Note
    ///
    /// This method has no way to determine whether [`EBUSY`] is intermittently returned because
    /// the TAP device is currently being destroyed or whether the TAP device is up and running
    /// normally (e.g., in use by a sandbox or even a snapshot).
    ///
    /// It is up to the caller to choose it over [`TapBuilder::build`] in the right context; e.g.:
    /// - when reinstating snapshots, which is the primary use for it, for now;
    /// - when pre-allocating [`Tap`]s in [`PoolTapDevices`] (faascell initializes sandbox
    ///   networking provider _before_ snapshots' reinstatement), where some of these [`Tap`]s
    ///   may already exist due to snapshots (which are not yet reinstated).
    ///
    /// # Errors
    ///
    /// - [`Error::TapAdopt`] containing the source [`Error`] is returned in case of failure to
    ///   adopt an existing TAP device (i.e., after [`ioctl(TUNSETIFF)`] has returned [`EBUSY`]).
    ///   This can happen on `rtnetlink` failure while querying for the device.
    /// - [`Error::TapBuild`] containing the source [`Error`] is returned in case of failure to
    ///   build a new TAP device (for some reason other than [`EBUSY`]).
    ///
    /// [`EBUSY`]: ::rustix::io::Errno::BUSY
    /// [`ioctl(TUNSETIFF)`]: ioctl::tun_set_iff
    /// [`PoolTapDevices`]: crate::network::providers::pool_tap::PoolTapDevices
    #[inline]
    pub(crate) async fn build_or_adopt(mut self) -> Result<TapDevice, Error> {
        match new_tap(
            &self.rtnl,
            self.info.name.clone(),
            self.info.ip_addr,
            self.info.prefix_len,
            self.info.gateway,
            self.info.mac_addr,
        )
        .await
        {
            Ok(tap) => Ok(tap),
            Err(Error::TapInit(e))
                if matches!(e.as_ref(), Error::TunSetIff(::rustix::io::Errno::BUSY)) =>
            {
                debug!(tap = ?self.info, "TAP device busy; adopting it...");
                let link = Link::by_name(&self.rtnl, self.info.name.as_str())
                    .await
                    .map_err(|err| Error::TapAdopt {
                        name: self.info.name.as_str().into(),
                        ip_addr: self.info.ip_addr,
                        prefix_len: self.info.prefix_len,
                        source: Box::new(err),
                    })?;
                if self.info.mac_addr.is_none() {
                    // If there is no MAC address stored in TapInfo, update that too.
                    self.info.mac_addr = Some(link.mac_addr()?);
                }
                Ok(TapDevice {
                    index: link.index(),
                    info: self.info,
                })
            }
            Err(err) => Err(Error::TapBuild {
                ip_addr: self.info.ip_addr,
                prefix_len: self.info.prefix_len,
                source: Box::new(err),
            }),
        }
    }
}

impl network::Resource for TapBuilder {
    type Descriptor = TapInfo;
}

/// # Note
///
/// This is serialized to and deserialized from a [`TapDevice`]; attempting to serialize or
/// deserialize the [`Tap::Builder`] variant always produces an error. In particular, we use:
/// - [`#[serde(skip)]`][1] on the [`Tap::Builder`] variant;
/// - [`#[serde(untagged)]`][2] on the [`Tap::Device`] variant.
///
/// [1]: https://serde.rs/variant-attrs.html#skip
/// [2]: https://serde.rs/variant-attrs.html#untagged
#[derive(Serialize, Deserialize)]
pub enum Tap {
    // Attempting either to serialize or to deserialize this variant always produces an error.
    #[serde(skip)]
    Builder(TapBuilder),

    // Both serialization and deserialization of this variant skips the `{"Device": {...}}` tag;
    // i.e., this variant's serialization is the same as its included `TapDevice`'s, and vice
    // versa: `Tap::Device(TapDevice)` can be deserialized from a serialized `TapDevice`.
    #[serde(untagged)]
    Device(TapDevice),
}

impl ::std::fmt::Debug for Tap {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            Self::Device(t) => write!(f, "{t:?}"),
            Self::Builder(t) => write!(f, "{t:?}"),
        }
    }
}

impl network::Resource for Tap {
    type Descriptor = TapInfo;
}

impl Tap {
    #[inline]
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Device(t) => &t.info.name,
            Self::Builder(t) => &t.info.name,
        }
    }

    #[inline]
    pub(crate) fn ip_addr(&self) -> Ipv4Addr {
        match self {
            Self::Device(t) => t.info.ip_addr,
            Self::Builder(t) => t.info.ip_addr,
        }
    }

    #[inline]
    pub(crate) fn prefix_len(&self) -> u8 {
        match self {
            Self::Device(t) => t.info.prefix_len,
            Self::Builder(t) => t.info.prefix_len,
        }
    }
}

::static_assertions::assert_impl_all!(TapDevice: Send);
::static_assertions::assert_impl_all!(TapBuilder: Send);
::static_assertions::assert_impl_all!(Tap: Send);

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use tokio::time::Instant;
    use tracing::info;
    use tracing_test::traced_test;

    use crate::network::DEVGROUP;

    use super::*;

    async fn test_name(rtnl: &::rtnetlink::Handle, tap_name: &str) -> ::anyhow::Result<TapDevice> {
        let t0 = Instant::now();
        let tap = TapDevice::new(
            rtnl,
            CompactString::from(tap_name),
            Ipv4Addr::new(10, 42, 42, 42),
            24,
            Ipv4Addr::new(10, 42, 42, 1),
            None,
        )
        .await
        .with_context(|| "failed to create new tap")?;
        info!(
            "Took {} to create: {tap:#?}",
            ::humantime::format_duration(t0.elapsed())
        );
        info!("tap's mac: {:x?}", tap.info.mac_addr);

        let link = Link::by_name(rtnl, tap.name())
            .await
            .with_context(|| format!("failed to look up link '{}'", tap.name()))?;
        let new_mac = link
            .mac_addr()
            .with_context(|| "failed to look up new Link's MAC address")?;
        info!("new Link's mac: {new_mac}");

        assert_eq!(
            tap.info.mac_addr,
            Some(new_mac),
            "MAC addr queried via rtnl != tap.mac_addr; systemd? is the .link file in place?"
        );

        assert_eq!(Some(DEVGROUP), link.group(), "incorrect link group");

        Ok(tap)
    }

    #[::tokio::test]
    #[traced_test]
    async fn name_tap01() -> ::anyhow::Result<()> {
        const TAP_NAME: &str = "uvm-test.tap01";

        let (conn, rtnl, _) = ::rtnetlink::new_connection()
            .with_context(|| "could not create new rtnetlink connection")?;
        let conn_task = ::tokio::spawn(conn);

        let tap = test_name(&rtnl, TAP_NAME).await?;
        let t0 = Instant::now();
        tap.destroy(&rtnl)
            .await
            .with_context(|| format!("could not destroy tap '{TAP_NAME}'"))?;
        info!(
            "Took {} to destroy tap",
            ::humantime::format_duration(t0.elapsed())
        );

        conn_task.abort();
        Ok(())
    }

    #[::tokio::test]
    #[traced_test]
    async fn name_tap01_wwwwwww() -> ::anyhow::Result<()> {
        const TAP_NAME: &str = "uvm-test.tap01WWWWWWW";

        let (conn, rtnl, _) = ::rtnetlink::new_connection()
            .with_context(|| "could not create new rtnetlink connection")?;
        let conn_task = ::tokio::spawn(conn);

        let tap = test_name(&rtnl, TAP_NAME).await?;
        let t0 = Instant::now();
        tap.destroy(&rtnl)
            .await
            .with_context(|| format!("could not destroy tap '{TAP_NAME}'"))?;
        info!(
            "Took {} to destroy tap",
            ::humantime::format_duration(t0.elapsed())
        );

        conn_task.abort();
        Ok(())
    }

    #[::tokio::test]
    #[traced_test]
    async fn name_t1() -> ::anyhow::Result<()> {
        const TAP_NAME: &str = "uvm-test.t1";

        let (conn, rtnl, _) = ::rtnetlink::new_connection()
            .with_context(|| "could not create new rtnetlink connection")?;
        let conn_task = ::tokio::spawn(conn);

        let tap = test_name(&rtnl, TAP_NAME).await?;
        let t0 = Instant::now();
        tap.destroy(&rtnl)
            .await
            .with_context(|| format!("could not destroy tap '{TAP_NAME}'"))?;
        info!(
            "Took {} to destroy tap",
            ::humantime::format_duration(t0.elapsed())
        );

        conn_task.abort();
        Ok(())
    }

    #[::tokio::test]
    #[traced_test]
    async fn test_set_mac_addr() -> ::anyhow::Result<()> {
        const TAP_NAME: &str = "uvm-test.set-mac-addr";

        let (conn, rtnl, _) = ::rtnetlink::new_connection()
            .with_context(|| "could not create new rtnetlink connection")?;
        let conn_task = ::tokio::spawn(conn);

        let mac_addr = MacAddress::new([0xAA, 0xFC, 0x00, 0x00, 0x05, 0xee]);

        let t0 = Instant::now();
        let tap = TapDevice::new(
            &rtnl,
            CompactString::from(TAP_NAME),
            Ipv4Addr::new(10, 42, 42, 42),
            24,
            Ipv4Addr::new(10, 42, 42, 1),
            Some(mac_addr),
        )
        .await
        .with_context(|| "failed to create new tap")?;
        info!(
            "Took {} to create: {tap:#?}",
            ::humantime::format_duration(t0.elapsed())
        );
        info!("tap's mac: {:x?}", tap.info.mac_addr);

        let link = Link::by_name(&rtnl, tap.name())
            .await
            .with_context(|| format!("failed to look up link '{}'", tap.name()))?;
        let new_mac = link
            .mac_addr()
            .with_context(|| "failed to look up new Link's MAC address")?;
        info!("new Link's mac: {new_mac}");

        assert_eq!(
            tap.info.mac_addr,
            Some(new_mac),
            "MAC addr queried via rtnl != tap.mac_addr; systemd? is the .link file in place?"
        );
        assert_eq!(
            mac_addr, new_mac,
            "MAC addr queried via rtnl is different than the one asked"
        );

        assert_eq!(Some(DEVGROUP), link.group(), "incorrect link group");

        let t0 = Instant::now();
        tap.destroy(&rtnl)
            .await
            .with_context(|| format!("could not destroy tap '{TAP_NAME}'"))?;
        info!(
            "Took {} to destroy tap",
            ::humantime::format_duration(t0.elapsed())
        );

        conn_task.abort();

        Ok(())
    }
}
