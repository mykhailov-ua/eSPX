variable "kubeconfig_path" {
  type        = string
  description = "Local kubeconfig path for k3s."
  default     = "~/.kube/config-espx"
}

variable "namespace" {
  type        = string
  description = "Kubernetes namespace for eSPX cold-path workloads."
  default     = "espx"
}

variable "k8s_manifests_path" {
  type        = string
  description = "Relative path from this env to deploy/k8s (base + apps manifests)."
  default     = "../../../k8s"
}

variable "container_image" {
  type        = string
  description = "eSPX distroless image tag imported into k3s containerd."
  default     = "ad-event-processor:latest"
}

variable "geoip_host_path" {
  type        = string
  description = "hostPath directory on the k3s node for MaxMind mmdb (synced by k8s_cold_path_up.sh)."
  default     = "/var/lib/espx/geoip"
}

variable "enable_hot_path" {
  type        = bool
  description = "When true, apply hostNetwork tracker/nginx manifests into espx-edge."
  default     = false
}

variable "hot_path_data_host" {
  type        = string
  description = "Loopback or host IP for Postgres/Redis from hostNetwork pods (local compose uses 127.0.0.1)."
  default     = "127.0.0.1"
}
