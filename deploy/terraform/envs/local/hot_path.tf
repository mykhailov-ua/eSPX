# Optional hot-path apply for local k3s (tracker x4 + OpenResty). Disabled by default.
resource "kubectl_manifest" "hot_path_namespace" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [module.k3s]

  yaml_body = file("${local.k8s_root}/hot-path/namespace.yaml")
}

resource "kubectl_manifest" "hot_path_configmap" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [kubectl_manifest.hot_path_namespace]

  yaml_body = templatefile("${local.k8s_root}/hot-path/configmap-env.yaml.tpl", {
    host_ip = var.hot_path_data_host
  })
}

resource "kubectl_manifest" "hot_path_secret" {
  count = var.enable_hot_path ? 1 : 0

  depends_on = [kubectl_manifest.hot_path_namespace]

  yaml_body = templatefile("${local.k8s_root}/hot-path/secret-env.yaml.tpl", {
    host_ip         = var.hot_path_data_host
    redis_password  = "your_redis_password_here"
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
