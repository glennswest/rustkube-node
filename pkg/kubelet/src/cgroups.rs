//! The cgroup driver and a Pod's cgroup parent on a CRI runtime (#24).
//!
//! Upstream's kubelet places each Pod under `kubepods`, one level per QoS
//! class, and names that place for the runtime in `LinuxPodSandboxConfig.
//! cgroup_parent`, spelled the way the runtime's cgroup driver wants it: a
//! systemd slice (`kubepods-burstable-pod<uid>.slice`, where systemd reads
//! each `-` as a level) or a cgroupfs path (`/kubepods/burstable/pod<uid>`).
//! The kubelet sent none, so a CRI runtime put every Pod at its own default.
//!
//! The driver is the runtime's, when it says (CRI `RuntimeConfig`, upstream's
//! way since 1.36), else `--cgroup-driver`, else `cgroupfs` (upstream's
//! default). stormpump owns its own groups (#57) and takes no parent from here.

/// How the runtime names cgroups.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CgroupDriver {
    #[default]
    Cgroupfs,
    Systemd,
}

impl CgroupDriver {
    /// `--cgroup-driver`'s spelling.
    pub fn parse(s: &str) -> Option<CgroupDriver> {
        match s.trim() {
            "cgroupfs" => Some(CgroupDriver::Cgroupfs),
            "systemd" => Some(CgroupDriver::Systemd),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CgroupDriver::Cgroupfs => "cgroupfs",
            CgroupDriver::Systemd => "systemd",
        }
    }
}

/// The Pod's cgroup parent as upstream names it for `driver`, from its QoS
/// class (`Guaranteed`, `Burstable`, `BestEffort`) and uid. An unknown class
/// (a sandbox config built without one) is `""`: the runtime's default.
pub fn pod_cgroup_parent(driver: CgroupDriver, qos: &str, uid: &str) -> String {
    let tier = match qos {
        "Guaranteed" => "",
        "Burstable" => "burstable",
        "BestEffort" => "besteffort",
        _ => return String::new(),
    };
    match driver {
        CgroupDriver::Systemd => {
            // systemd reads `-` as a level, so the uid's own dashes go.
            let uid = uid.replace('-', "_");
            if tier.is_empty() {
                format!("kubepods-pod{uid}.slice")
            } else {
                format!("kubepods-{tier}-pod{uid}.slice")
            }
        }
        CgroupDriver::Cgroupfs => {
            if tier.is_empty() {
                format!("/kubepods/pod{uid}")
            } else {
                format!("/kubepods/{tier}/pod{uid}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #24: upstream's names, per driver and QoS class.
    #[test]
    fn pod_parents_are_upstreams() {
        let uid = "5c0e-7a";
        let sd = CgroupDriver::Systemd;
        assert_eq!(pod_cgroup_parent(sd, "Guaranteed", uid), "kubepods-pod5c0e_7a.slice");
        assert_eq!(pod_cgroup_parent(sd, "Burstable", uid), "kubepods-burstable-pod5c0e_7a.slice");
        assert_eq!(pod_cgroup_parent(sd, "BestEffort", uid), "kubepods-besteffort-pod5c0e_7a.slice");
        let fs = CgroupDriver::Cgroupfs;
        assert_eq!(pod_cgroup_parent(fs, "Guaranteed", uid), "/kubepods/pod5c0e-7a");
        assert_eq!(pod_cgroup_parent(fs, "Burstable", uid), "/kubepods/burstable/pod5c0e-7a");
        assert_eq!(pod_cgroup_parent(fs, "BestEffort", uid), "/kubepods/besteffort/pod5c0e-7a");
        assert_eq!(pod_cgroup_parent(sd, "", uid), "");
        assert_eq!(CgroupDriver::parse("systemd"), Some(sd));
        assert_eq!(CgroupDriver::parse("cgroupfs"), Some(fs));
        assert_eq!(CgroupDriver::parse("sysd"), None);
    }
}
