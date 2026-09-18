use std::net::Ipv4Addr;

use mac_address::MacAddress;
use protobuf::MessageField;

use firecracker_containerd_ttrpc::types::{
    FirecrackerNetworkInterface, IPConfiguration, StaticNetworkConfiguration,
};

#[derive(Debug, Clone)]
pub struct NetworkInterfaceBuilder {
    pub guest_mac_addr: Option<MacAddress>,
    pub host_device_name: String,
    pub vm_ip_address: Ipv4Addr,
    pub prefix_length: u8,
    pub vm_gateway: Ipv4Addr,
    pub nameservers: Vec<Ipv4Addr>,
}

impl NetworkInterfaceBuilder {
    /// Construct a new instance of a `NetworkInterfaceBuilder`, setting its mandatory fields.
    ///
    /// # Arguments
    ///
    /// - `host_device_name`: the tap device's name on the host.
    /// - `vm_ip_address` and `prefix_length`: the IP address to be assigned to the VM's network
    ///   interface by the guest kernel, along with the prefix length.
    /// - `vm_gateway`: the IP address of the default gateway for the guest kernel.
    #[inline]
    pub fn new(
        host_device_name: impl Into<String>,
        vm_ip_address: impl Into<Ipv4Addr>,
        prefix_length: u8,
        vm_gateway: impl Into<Ipv4Addr>,
    ) -> Self {
        Self {
            guest_mac_addr: None,
            host_device_name: host_device_name.into(),
            vm_ip_address: vm_ip_address.into(),
            prefix_length,
            vm_gateway: vm_gateway.into(),
            nameservers: Vec::new(),
        }
    }

    /// Add the MAC address of the VM's network (`virtio-net`) device; i.e., the guest MAC address.
    ///
    /// In Firecracker's API, the corresponding field (`guest_mac`) is _optional_; when ommited,
    /// Firecracker assigns a (locally administered, unicast) MAC to the `virtio-net` device, in
    /// conformance with [virtio-v1.2 spec, §5.1.4.2][virtio1.2-5.1.4.2]).
    ///
    /// [virtio1.2-5.1.4.2]: https://docs.oasis-open.org/virtio/virtio/v1.2/cs01/virtio-v1.2-cs01.html#x1-2250002
    #[inline]
    pub fn guest_mac_addr(mut self, mac_address: impl Into<MacAddress>) -> Self {
        self.guest_mac_addr = Some(mac_address.into());
        self
    }

    /// Add the IP addresses of the nameservers to be passed to the guest kernel.
    #[inline]
    pub fn nameservers(
        mut self,
        nameservers: impl IntoIterator<Item = impl Into<Ipv4Addr>>,
    ) -> Self {
        self.nameservers = nameservers.into_iter().map(|ip| ip.into()).collect();
        self
    }

    /// Consume this builder, returning a new [`FirecrackerNetworkInterface`].
    pub fn build(self) -> FirecrackerNetworkInterface {
        FirecrackerNetworkInterface {
            StaticConfig: MessageField::some(StaticNetworkConfiguration {
                MacAddress: self
                    .guest_mac_addr
                    .map(|addr| addr.to_string())
                    .unwrap_or_default(),
                HostDevName: self.host_device_name,
                IPConfig: MessageField::some(IPConfiguration {
                    PrimaryAddr: format!("{}/{}", self.vm_ip_address, self.prefix_length),
                    GatewayAddr: self.vm_gateway.to_string(),
                    Nameservers: self
                        .nameservers
                        .into_iter()
                        .take(2)
                        .map(|ns| ns.to_string())
                        .collect(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}
