variable "kubeconfig_path" {
  description = "Admin kubeconfig written by infra/terraform/talos (output kubeconfig_path)."
  type        = string
}

variable "install_cilium" {
  description = "Install Cilium and Hubble: full stand only (Talos is configured without CNI and kube-proxy)."
  type        = bool
  default     = false
}

variable "k8s_service_host" {
  description = "Kubernetes API address for Cilium (no kube-proxy): the control plane or load balancer address."
  type        = string
  default     = ""
}

variable "k8s_service_port" {
  type    = number
  default = 6443
}

variable "cilium_chart_version" {
  type    = string
  default = "1.20.5"
}

variable "argocd_chart_version" {
  description = "Same version as ARGOCD_CHART_VERSION in infra/bootstrap/bootstrap.sh."
  type        = string
  default     = "10.18.2"
}

variable "argocd_apps_chart_version" {
  description = "Chart argocd-apps: creates the root Application after the Argo CD CRDs exist."
  type        = string
  default     = "2.0.8"
}

variable "gitops_repo_url" {
  type    = string
  default = "https://github.com/m5tshift/weigate-gitops"
}

variable "gitops_revision" {
  type    = string
  default = "main"
}
