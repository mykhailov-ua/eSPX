output "host_ip" {
  value       = local.host_ip
  description = "Node InternalIP used in DSN and REDIS_ADDRS for compose-backed data plane."
}

output "kubeconfig_path" {
  value       = module.k3s.kubeconfig_path
  description = "Kubeconfig for kubectl and CI."
}

output "namespace" {
  value       = var.namespace
  description = "eSPX cold-path namespace."
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
  description = "Deployments managed under namespace espx (not tracker/nginx)."
}

output "kubectl_context_hint" {
  value       = "export KUBECONFIG=${local.kubeconfig} && kubectl get deploy -n ${var.namespace}"
  description = "Quick verify command after apply."
}
