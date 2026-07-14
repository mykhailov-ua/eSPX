# Optional hot-path on staging edge nodes (hostNetwork trackers → external Redis).
resource "kubectl_manifest" "hot_path_namespace" {
  count = var.enable_hot_path ? 1 : 0

  yaml_body = file("${local.k8s_root}/hot-path/namespace.yaml")
}

resource "kubectl_manifest" "hot_path_configmap" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [kubectl_manifest.hot_path_namespace]

  yaml_body = templatefile("${local.k8s_root}/hot-path/configmap-env.staging.tpl", {
    env               = var.env
    redis_addrs       = var.redis_addrs
    filter_timeout_ms = var.filter_timeout_ms
    udp_mgmt_addr     = "management.${var.namespace}.svc.cluster.local:8190"
  })
}

resource "kubectl_manifest" "hot_path_secret" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [kubectl_manifest.hot_path_namespace]

  yaml_body = templatefile("${local.k8s_root}/hot-path/secret-env.staging.tpl", {
    db_dsn              = var.db_dsn
    redis_password      = var.redis_password
    token_symmetric_key = var.token_symmetric_key
  })
}

resource "kubectl_manifest" "hot_path_trackers" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [
    kubectl_manifest.hot_path_configmap,
    kubectl_manifest.hot_path_secret,
  ]

  yaml_body = templatefile("${local.k8s_root}/hot-path/deployment-trackers.yaml.tpl", {
    geoip_host_path = local.geoip_host_path
  })
  wait_for_rollout = false
}

resource "kubectl_manifest" "hot_path_nginx" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [kubectl_manifest.hot_path_namespace]

  yaml_body        = file("${local.k8s_root}/hot-path/daemonset-nginx.yaml")
  wait_for_rollout = false
}
