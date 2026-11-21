# Chart version and values are the ones of infra/bootstrap.
resource "helm_release" "argocd" {
  name       = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  version    = var.argocd_chart_version
  namespace  = kubernetes_namespace_v1.ns["argocd"].metadata[0].name
  wait       = true
  timeout    = 600

  values = [file("${path.module}/../../bootstrap/argocd-values.yaml")]
}

# Root Application (App of Apps, SRS INF-2.2) via argocd-apps: a plain manifest would need the CRD at plan time.
resource "helm_release" "root" {
  name       = "argocd-root"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = var.argocd_apps_chart_version
  namespace  = "argocd"
  wait       = true

  values = [yamlencode({
    applications = {
      root = {
        namespace = "argocd"
        project   = "default"
        annotations = {
          "argocd.argoproj.io/sync-wave" = "0"
        }
        source = {
          repoURL        = var.gitops_repo_url
          targetRevision = var.gitops_revision
          path           = "apps"
          directory      = { recurse = true, exclude = "root.yaml" }
        }
        destination = {
          server    = "https://kubernetes.default.svc"
          namespace = "argocd"
        }
        syncPolicy = {
          automated   = { prune = true, selfHeal = true }
          syncOptions = ["ServerSideApply=true"]
          retry       = { limit = 10, backoff = { duration = "10s", factor = 2, maxDuration = "3m" } }
        }
      }
    }
  })]

  depends_on = [helm_release.argocd]
}
