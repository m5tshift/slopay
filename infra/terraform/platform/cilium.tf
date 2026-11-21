# Cilium replaces kube-proxy (SRS INF-1.1a). Talos: cgroup is mounted by the host, API is reached directly.
resource "helm_release" "cilium" {
  count = var.install_cilium ? 1 : 0

  name       = "cilium"
  repository = "https://helm.cilium.io"
  chart      = "cilium"
  version    = var.cilium_chart_version
  namespace  = "kube-system"
  wait       = true
  timeout    = 600

  values = [yamlencode({
    kubeProxyReplacement = true
    k8sServiceHost       = var.k8s_service_host
    k8sServicePort       = var.k8s_service_port
    ipam                 = { mode = "kubernetes" }
    cni                  = { exclusive = false }
    socketLB             = { hostNamespaceOnly = true }
    cgroup               = { autoMount = { enabled = false }, hostRoot = "/sys/fs/cgroup" }
    securityContext = {
      capabilities = {
        ciliumAgent      = ["CHOWN", "KILL", "NET_ADMIN", "NET_RAW", "IPC_LOCK", "SYS_ADMIN", "SYS_RESOURCE", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID"]
        cleanCiliumState = ["NET_ADMIN", "SYS_ADMIN", "SYS_RESOURCE"]
      }
    }
    hubble = {
      enabled = true
      relay   = { enabled = true }
      ui      = { enabled = true }
    }
  })]
}
