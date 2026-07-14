output "namespace" {
  value       = var.namespace
  description = "eSPX cold-path namespace."
}

output "container_image" {
  value       = var.container_image
  description = "Image applied to cold-path Deployments."
}

output "redis_addrs" {
  value       = var.redis_addrs
  description = "Redis shard endpoints wired into ConfigMap."
  sensitive   = true
}

output "cold_path_services" {
  value = [
    "auth",
    "management",
    "payment",
    "billing",
    "notifier",
    "processor",
    "ivt-detector",
  ]
  description = "Cold-path Deployments (hot path stays hostNetwork outside this apply)."
}

output "kubectl_context_hint" {
  value       = "export KUBECONFIG=${local.kubeconfig} && kubectl get deploy -n ${var.namespace}"
  description = "Quick verify after apply."
}
