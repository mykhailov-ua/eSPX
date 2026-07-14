output "kubeconfig_path" {
  value       = var.kubeconfig_path
  description = "Kubeconfig path used by kubectl and kubernetes providers."
}

output "k3s_ready" {
  value       = try(data.external.k3s_ready.result.ready, "false")
  description = "True when kubeconfig file exists after bootstrap."
}
