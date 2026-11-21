mock_provider "kubernetes" {}
mock_provider "helm" {}

variables {
  kubeconfig_path  = "/nonexistent/kubeconfig"
  install_cilium   = true
  k8s_service_host = "192.0.2.11"
}

run "full_stand" {
  command = plan

  assert {
    condition     = length(helm_release.cilium) == 1 && contains(keys(kubernetes_namespace_v1.ns), "apps") && contains(keys(kubernetes_namespace_v1.ns), "argocd")
    error_message = "Cilium, namespaces apps and argocd are expected"
  }
}

run "minimal_stand" {
  command = plan

  variables {
    install_cilium = false
  }

  assert {
    condition     = length(helm_release.cilium) == 0
    error_message = "Cilium must not be installed on the minimal stand"
  }
}
