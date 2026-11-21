# The namespace list is the one of infra/bootstrap (SRS 4.4); it is not copied.
locals {
  namespaces = {
    for doc in split("\n---\n", file("${path.module}/../../bootstrap/namespaces.yaml")) :
    yamldecode(doc).metadata.name => try(yamldecode(doc).metadata.labels, {})
  }
}

resource "kubernetes_namespace_v1" "ns" {
  for_each = local.namespaces

  metadata {
    name   = each.key
    labels = each.value
  }

  depends_on = [helm_release.cilium]
}

# Read-only access of the agent in apps (infra/bootstrap/agent-rbac.yaml): ServiceAccount, Role, RoleBinding.
resource "kubernetes_service_account_v1" "agent_reader" {
  metadata {
    name      = "agent-reader"
    namespace = kubernetes_namespace_v1.ns["apps"].metadata[0].name
  }
}

resource "kubernetes_role_v1" "agent_reader" {
  metadata {
    name      = "agent-reader"
    namespace = kubernetes_namespace_v1.ns["apps"].metadata[0].name
  }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "events", "services", "endpoints", "persistentvolumeclaims", "serviceaccounts", "resourcequotas", "limitranges"]
    verbs      = ["get", "list", "watch"]
  }
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "replicasets", "statefulsets", "daemonsets"]
    verbs      = ["get", "list", "watch"]
  }
  rule {
    api_groups = ["batch"]
    resources  = ["jobs", "cronjobs"]
    verbs      = ["get", "list", "watch"]
  }
  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["networkpolicies", "ingresses"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding_v1" "agent_reader" {
  metadata {
    name      = "agent-reader"
    namespace = kubernetes_namespace_v1.ns["apps"].metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.agent_reader.metadata[0].name
    namespace = kubernetes_namespace_v1.ns["apps"].metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.agent_reader.metadata[0].name
  }
}
