//! Taps, and the bridge they hang off.
//!
//! **A hypervisor must never be able to make its own NIC.** Creating a tap
//! needs `CAP_NET_ADMIN`, and a VMM that could do it would hold that
//! capability for the life of the guest — over a process that runs a whole
//! foreign operating system. So the node makes the tap, keeps the privilege,
//! and hands over only the *descriptor*: stormpump carries it across the ring
//! socket with `SCM_RIGHTS` and `dup2`s it into place before `execve`, and the
//! hypervisor inherits something it could not have opened.
//!
//! That is the same shape as the log files a workload already inherits, and it
//! is why `Spec.fds` exists.
//!
//! # What is here and what is not
//!
//! This is the `bridged` case: a tap on a named Linux bridge on the node. It
//! is the mechanism the others reuse —
//!
//! - **`pod`** (a VM with a Cilium identity) is the same tap, made inside a
//!   sandbox's network namespace and bridged to the veth the CNI put there.
//! - **`nad`** resolves a NetworkAttachmentDefinition to a bridge and lands
//!   back here.
//!
//! # No netlink library
//!
//! `ioctl` and the `/sys` tree, because the whole job is four operations —
//! does a bridge exist, make one, make a tap, enslave it — and a netlink crate
//! is a dependency the node would carry for those four.

#[cfg(target_os = "linux")]
mod linux {
    use std::os::fd::{AsRawFd, OwnedFd};

    /// `struct ifreq`, the part of it these calls use.
    #[repr(C)]
    struct IfReq {
        name: [u8; 16],
        /// Union in C; only the flags arm is used here.
        flags: i16,
        _pad: [u8; 22],
    }

    const TUNSETIFF: libc::c_ulong = 0x4004_54ca;
    const SIOCBRADDBR: libc::c_ulong = 0x89a0;
    const SIOCBRADDIF: libc::c_ulong = 0x89a2;
    const SIOCGIFINDEX: libc::c_ulong = 0x8933;
    const SIOCSIFFLAGS: libc::c_ulong = 0x8914;
    const SIOCGIFFLAGS: libc::c_ulong = 0x8913;

    const IFF_TAP: i16 = 0x0002;
    const IFF_NO_PI: i16 = 0x1000;
    /// vhost-net needs the virtio header on the tap, and qemu asks for it when
    /// `vhost=on`. Without it the two disagree about the frame layout and the
    /// guest sees no traffic at all.
    const IFF_VNET_HDR: i16 = 0x4000;
    const IFF_UP: i16 = 0x1;

    fn ifreq(name: &str) -> Result<IfReq, String> {
        if name.len() >= 16 {
            return Err(format!("{name}: an interface name is at most 15 bytes"));
        }
        let mut r = IfReq { name: [0; 16], flags: 0, _pad: [0; 22] };
        r.name[..name.len()].copy_from_slice(name.as_bytes());
        Ok(r)
    }

    fn ctl_socket() -> Result<OwnedFd, String> {
        // SAFETY: a datagram socket; -1 on failure, checked.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(format!("socket: {}", std::io::Error::last_os_error()));
        }
        // SAFETY: a descriptor this process just created and owns.
        Ok(unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) })
    }

    /// Whether an interface exists, asked of the kernel rather than of a cache.
    pub fn exists(name: &str) -> bool {
        std::path::Path::new(&format!("/sys/class/net/{name}")).exists()
    }

    /// Make sure `bridge` exists and is up.
    ///
    /// Idempotent: two VMs starting at once both ask, and the second must find
    /// the first's bridge rather than fail. An existing interface that is not
    /// a bridge is an error rather than something to use — putting a tap on a
    /// physical NIC would take the node's own network away.
    pub fn ensure_bridge(bridge: &str) -> Result<(), String> {
        if exists(bridge) {
            if !std::path::Path::new(&format!("/sys/class/net/{bridge}/bridge")).exists() {
                return Err(format!("{bridge} exists and is not a bridge"));
            }
            return up(bridge);
        }
        let sock = ctl_socket()?;
        let mut name = [0u8; 16];
        name[..bridge.len()].copy_from_slice(bridge.as_bytes());
        // SAFETY: SIOCBRADDBR takes the name buffer directly.
        let rc = unsafe { libc::ioctl(sock.as_raw_fd(), SIOCBRADDBR as _, name.as_ptr()) };
        if rc != 0 {
            let e = std::io::Error::last_os_error();
            // EEXIST: somebody else won the race, which is success.
            if e.raw_os_error() != Some(libc::EEXIST) {
                return Err(format!("creating bridge {bridge}: {e}"));
            }
        }
        up(bridge)
    }

    /// Bring an interface up.
    pub fn up(name: &str) -> Result<(), String> {
        let sock = ctl_socket()?;
        let mut req = ifreq(name)?;
        // SAFETY: ifreq is correctly sized for both calls.
        unsafe {
            if libc::ioctl(sock.as_raw_fd(), SIOCGIFFLAGS as _, &mut req) != 0 {
                return Err(format!("{name}: reading flags: {}", std::io::Error::last_os_error()));
            }
            req.flags |= IFF_UP;
            if libc::ioctl(sock.as_raw_fd(), SIOCSIFFLAGS as _, &req) != 0 {
                return Err(format!("{name}: bringing up: {}", std::io::Error::last_os_error()));
            }
        }
        Ok(())
    }

    /// Create a tap, attach it to `bridge`, bring it up, and return its
    /// descriptor.
    ///
    /// The descriptor is what matters: the interface is torn down by the
    /// kernel when the last reference to it goes, so a VM that exits takes its
    /// tap with it and nothing has to remember to clean up.
    pub fn tap_on_bridge(name: &str, bridge: &str) -> Result<OwnedFd, String> {
        ensure_bridge(bridge)?;

        let tun = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .map_err(|e| format!("/dev/net/tun: {e} — is the tun module loaded?"))?;
        let mut req = ifreq(name)?;
        req.flags = IFF_TAP | IFF_NO_PI | IFF_VNET_HDR;
        // SAFETY: TUNSETIFF with a correctly sized ifreq on a tun descriptor.
        if unsafe { libc::ioctl(tun.as_raw_fd(), TUNSETIFF as _, &mut req) } != 0 {
            return Err(format!("creating tap {name}: {}", std::io::Error::last_os_error()));
        }

        // Enslave it. SIOCBRADDIF takes the *index* of the interface to add,
        // in the ifreq's second field — the name field names the bridge.
        let sock = ctl_socket()?;
        let mut idx = ifreq(name)?;
        // SAFETY: SIOCGIFINDEX fills the union's index arm.
        if unsafe { libc::ioctl(sock.as_raw_fd(), SIOCGIFINDEX as _, &mut idx) } != 0 {
            return Err(format!("{name}: index: {}", std::io::Error::last_os_error()));
        }
        let ifindex = idx.flags as i32 as u32; // the index lands where flags sits
        let mut add = ifreq(bridge)?;
        add.flags = ifindex as i16;
        // SAFETY: as above; the kernel reads the index from the union.
        if unsafe { libc::ioctl(sock.as_raw_fd(), SIOCBRADDIF as _, &add) } != 0 {
            return Err(format!(
                "attaching {name} to {bridge}: {}",
                std::io::Error::last_os_error()
            ));
        }
        up(name)?;
        Ok(tun.into())
    }
}

#[cfg(target_os = "linux")]
pub use linux::{ensure_bridge, exists, tap_on_bridge, up};

/// A MAC that is this VM's and stays this VM's.
///
/// Derived from the name rather than random: a VM that restarts keeps its
/// address, so its DHCP reservation, its ARP entries and anything keyed on the
/// MAC survive — and two nodes starting the same VM name cannot collide with
/// each other by chance.
///
/// `52:54:00` is QEMU's own OUI, locally administered; the rest is a hash of
/// the name, which is what makes it stable.
pub fn mac_for(vm: &str, nic: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in vm.bytes().chain(b":".iter().copied()).chain(nic.bytes()) {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    format!(
        "52:54:00:{:02x}:{:02x}:{:02x}",
        (h >> 16) as u8,
        (h >> 8) as u8,
        h as u8
    )
}

/// The interface name a tap gets on the node.
///
/// Bounded to 15 bytes, which is the kernel's limit and not a style choice: a
/// longer name fails at `TUNSETIFF` with EINVAL and nothing says why. A VM
/// called `some-very-long-name` with a nic called `eth0` has to fit, so the
/// name is a prefix plus a hash of the pair.
pub fn tap_name(vm: &str, nic: &str) -> String {
    let mut h: u32 = 2_166_136_261;
    for b in vm.bytes().chain(b'/'..=b'/').chain(nic.bytes()) {
        h ^= b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    format!("vm{h:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A restart must not change the address: a new MAC means a new DHCP
    /// lease, a new ARP entry and, on a network that reserves by MAC, a
    /// machine that has lost its identity.
    #[test]
    fn a_mac_is_stable_and_locally_administered() {
        let a = mac_for("web-1", "eth0");
        assert_eq!(a, mac_for("web-1", "eth0"), "the same VM must get the same MAC");
        assert_ne!(a, mac_for("web-2", "eth0"));
        assert_ne!(a, mac_for("web-1", "eth1"), "two NICs on one VM are two addresses");
        assert!(a.starts_with("52:54:00:"), "{a}");
        // Locally administered, and not a multicast address — the second bit
        // of the first byte set, the first bit clear.
        let first = u8::from_str_radix(&a[..2], 16).unwrap();
        assert_eq!(first & 0x01, 0, "a multicast MAC is not a host address");
        assert_eq!(first & 0x02, 0x02, "must be locally administered");
    }

    /// 15 bytes is the kernel's limit, and a longer name fails at TUNSETIFF
    /// with EINVAL and no explanation.
    #[test]
    fn a_tap_name_fits_the_kernels_limit_and_is_stable() {
        let long = tap_name("a-very-long-virtual-machine-name-indeed", "eth0");
        assert!(long.len() <= 15, "{long} is {} bytes", long.len());
        assert_eq!(long, tap_name("a-very-long-virtual-machine-name-indeed", "eth0"));
        assert_ne!(long, tap_name("a-very-long-virtual-machine-name-indeeD", "eth0"));
        assert_ne!(tap_name("vm", "eth0"), tap_name("vm", "eth1"));
    }
}
