# Binary package built from prebuilt release binaries — see
# packaging/build-packages.sh (copies binaries/units/env files into SOURCES).
%global debug_package %{nil}

Name:           rustkube-node
Version:        %{?pkg_version}%{!?pkg_version:0.1.0}
Release:        1%{?dist}
Summary:        RustKube node components: kubelet and kube-proxy (Rust)
License:        Apache-2.0
URL:            https://github.com/glennswest/rustkube-node
BuildRequires:  systemd-rpm-macros
Requires:       iproute
Recommends:     cri-o

%description
Drop-in Kubernetes node components in Rust from the rustkube project:
kubelet (node agent; CRI v1 over gRPC to CRI-O/containerd, native and
microVM runtimes) and kube-proxy (iptables service dataplane). Uses exact
upstream names and config paths under /etc/kubernetes.

%install
install -D -m0755 %{_sourcedir}/kubelet %{buildroot}%{_bindir}/kubelet
install -D -m0755 %{_sourcedir}/kube-proxy %{buildroot}%{_bindir}/kube-proxy
install -D -m0644 %{_sourcedir}/kubelet.service %{buildroot}%{_unitdir}/kubelet.service
install -D -m0644 %{_sourcedir}/kube-proxy.service %{buildroot}%{_unitdir}/kube-proxy.service
install -D -m0644 %{_sourcedir}/kubelet.env %{buildroot}%{_sysconfdir}/kubernetes/kubelet
install -D -m0644 %{_sourcedir}/kube-proxy.env %{buildroot}%{_sysconfdir}/kubernetes/kube-proxy

%post
%systemd_post kubelet.service kube-proxy.service

%preun
%systemd_preun kubelet.service kube-proxy.service

%postun
%systemd_postun_with_restart kubelet.service kube-proxy.service

%files
%{_bindir}/kubelet
%{_bindir}/kube-proxy
%{_unitdir}/kubelet.service
%{_unitdir}/kube-proxy.service
%config(noreplace) %{_sysconfdir}/kubernetes/kubelet
%config(noreplace) %{_sysconfdir}/kubernetes/kube-proxy

%changelog
* Wed Jul 15 2026 Glenn West <glennswest@neuralcloudcomputing.com> - 0.1.0-1
- Initial package: kubelet + kube-proxy binaries, systemd units, env configs
