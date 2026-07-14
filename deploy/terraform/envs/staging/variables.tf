variable "kubeconfig_path" {
  type        = string
  description = "Kubeconfig for the staging k3s cluster (remote VM or managed k8s)."
}

variable "namespace" {
  type        = string
  description = "Kubernetes namespace for eSPX cold-path workloads."
  default     = "espx"
}

variable "k8s_manifests_path" {
  type        = string
  description = "Relative path from this env to deploy/k8s."
  default     = "../../../k8s"
}

variable "container_image" {
  type        = string
  description = "Registry image (e.g. ghcr.io/org/espx:v1.2.3). Replaces ad-event-processor:latest."
}

variable "image_pull_policy" {
  type        = string
  description = "Pull policy for cold-path pods."
  default     = "Always"
}

variable "geoip_host_path" {
  type        = string
  description = "hostPath on processor nodes for MaxMind mmdb."
  default     = "/var/lib/espx/geoip"
}

variable "env" {
  type        = string
  description = "Runtime mode passed to ENV."
  default     = "production"
}

variable "filter_timeout_ms" {
  type        = string
  description = "Redis filter deadline; production SLA expects <= 100."
  default     = "100"
}

variable "redis_addrs" {
  type        = string
  description = "Comma-separated StaticSlotSharder masters (exactly four)."
}

variable "trusted_proxies" {
  type        = string
  description = "CIDR list for X-Forwarded-For trust behind L4/L7 edge."
  default     = "10.0.0.0/8,172.16.0.0/12"
}

variable "db_dsn" {
  type        = string
  description = "Main Postgres DSN."
  sensitive   = true
}

variable "payment_db_dsn" {
  type        = string
  description = "Payment schema Postgres DSN."
  sensitive   = true
}

variable "ch_dsn" {
  type        = string
  description = "ClickHouse DSN."
  sensitive   = true
}

variable "redis_password" {
  type        = string
  description = "Redis AUTH password shared across shards."
  sensitive   = true
  default     = ""
}

variable "payment_internal_token" {
  type      = string
  sensitive = true
}

variable "settlement_internal_token" {
  type      = string
  sensitive = true
}

variable "billing_internal_token" {
  type      = string
  sensitive = true
}

variable "token_symmetric_key" {
  type        = string
  description = "32-byte PASETO symmetric key."
  sensitive   = true
}

variable "admin_api_key" {
  type      = string
  sensitive = true
}

variable "stripe_secret_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "stripe_webhook_secret" {
  type      = string
  sensitive = true
  default   = ""
}

variable "telegram_bot_token" {
  type      = string
  sensitive = true
  default   = ""
}

variable "telegram_chat_id" {
  type    = string
  default = ""
}

variable "maxmind_license_key" {
  type      = string
  sensitive = true
  default   = ""
}

variable "registry_server" {
  type        = string
  description = "Container registry host (e.g. ghcr.io). Empty skips pull secret."
  default     = ""
}

variable "registry_username" {
  type      = string
  sensitive = true
  default   = ""
}

variable "registry_password" {
  type      = string
  sensitive = true
  default   = ""
}

variable "exclude_nodeports" {
  type        = bool
  description = "When true, skip NodePort Services (use Ingress on staging instead)."
  default     = true
}

variable "enable_ingress" {
  type        = bool
  description = "When true, apply ingress-cold-path.yaml.tpl."
  default     = true
}

variable "ingress_class" {
  type        = string
  description = "IngressClass name on the staging cluster."
  default     = "nginx"
}

variable "admin_host" {
  type        = string
  description = "DNS host for management admin HTTP."
  default     = "admin.example.com"
}

variable "payment_webhook_host" {
  type        = string
  description = "DNS host for Stripe webhooks (payment service)."
  default     = "pay.example.com"
}

variable "tls_secret_name" {
  type        = string
  description = "TLS secret in namespace for Ingress (cert-manager or manual)."
  default     = "espx-tls"
}

variable "udp_tracker_addrs" {
  type        = string
  description = "Comma-separated host:port for tracker UDP recv (8181-8184 on edge nodes)."
  default     = "127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183,127.0.0.1:8184"
}

variable "udp_mgmt_addr" {
  type        = string
  description = "Management UDP control plane reachable from hostNetwork trackers."
  default     = "management.espx.svc.cluster.local:8190"
}

variable "enable_hot_path" {
  type        = bool
  description = "When true, apply espx-edge tracker x4 against staging data plane."
  default     = false
}
