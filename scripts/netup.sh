#!/bin/bash
#
# ckatsak, Wed Jul 19 03:30:58 PM EEST 2023

set -o errexit
set -o nounset
set -o pipefail

HOST_IF="$1"

DEVGROUP="$((0xFAA5CE11))"

echo 1 >'/proc/sys/net/ipv4/ip_forward'

# Enable sending a gratuitous ARP in default iface configuration, so that it is
# inherited by all other interfaces (e.g., the TAPs), in case it is needed.
echo 1 >'/proc/sys/net/ipv4/conf/default/arp_notify'

iptables -t nat -A POSTROUTING -o "$HOST_IF" -j MASQUERADE
iptables -A FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
iptables -A FORWARD -o "$HOST_IF" -m devgroup --src-group "$DEVGROUP" -j ACCEPT
