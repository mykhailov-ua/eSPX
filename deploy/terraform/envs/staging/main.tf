locals {
  kubeconfig = pathexpand(var.kubeconfig_path)
  k8s_root   = abspath("${path.module}/${var.k8s_manifests_path}")

  k8s_apps_static = [
    for f in fileset("${local.k8s_root}/apps", "*.yaml") :
    "${local.k8s_root}/apps/${f}"
    if !endswith(f, "kustomization.yaml") && !(var.exclude_nodeports && endswith(f, "-nodeport.yaml"))
  ]

  geoip_host_path   = abspath(var.geoip_host_path)
  use_registry_auth = var.registry_server != "" && var.registry_password != ""

  secret_template_vars = {
    namespace                 = var.namespace
    db_dsn                    = var.db_dsn
    payment_db_dsn            = var.payment_db_dsn
    ch_dsn                    = var.ch_dsn
    redis_password            = var.redis_password
    payment_internal_token    = var.payment_internal_token
    settlement_internal_token = var.settlement_internal_token
    billing_internal_token    = var.billing_internal_token
    token_symmetric_key       = var.token_symmetric_key
    admin_api_key             = var.admin_api_key
    stripe_secret_key         = var.stripe_secret_key
    stripe_webhook_secret     = var.stripe_webhook_secret
    telegram_bot_token        = var.telegram_bot_token
    telegram_chat_id          = var.telegram_chat_id
    maxmind_license_key       = var.maxmind_license_key
  }

  processor_yaml = templatefile("${local.k8s_root}/apps/deployment-processor.yaml.tpl", {
    geoip_host_path = local.geoip_host_path
  })

  patched_apps_yaml = {
    for path in local.k8s_apps_static : path => (
      local.use_registry_auth ?
      replace(
        replace(
          replace(file(path), "ad-event-processor:latest", var.container_image),
          "imagePullPolicy: IfNotPresent",
          "imagePullPolicy: ${var.image_pull_policy}",
        ),
        "    spec:\n      containers:",
        "    spec:\n      imagePullSecrets:\n        - name: espx-registry\n      containers:",
      ) :
      replace(
        replace(file(path), "ad-event-processor:latest", var.container_image),
        "imagePullPolicy: IfNotPresent",
        "imagePullPolicy: ${var.image_pull_policy}",
      )
    )
  }

  patched_processor_yaml = (
    local.use_registry_auth ?
    replace(
      replace(
        replace(local.processor_yaml, "ad-event-processor:latest", var.container_image),
        "imagePullPolicy: IfNotPresent",
        "imagePullPolicy: ${var.image_pull_policy}",
      ),
      "    spec:\n      containers:",
      "    spec:\n      imagePullSecrets:\n        - name: espx-registry\n      containers:",
    ) :
    replace(
      replace(local.processor_yaml, "ad-event-processor:latest", var.container_image),
      "imagePullPolicy: IfNotPresent",
      "imagePullPolicy: ${var.image_pull_policy}",
    )
  )
}

provider "kubernetes" {
  config_path = local.kubeconfig
}

provider "kubectl" {
  config_path = local.kubeconfig
}

resource "kubernetes_secret" "registry" {
  count = local.use_registry_auth ? 1 : 0

  metadata {
    name      = "espx-registry"
    namespace = var.namespace
    labels = {
      "app.kubernetes.io/part-of" = "espx"
    }
  }

  type = "kubernetes.io/dockerconfigjson"

  data = {
    ".dockerconfigjson" = jsonencode({
      auths = {
        "${var.registry_server}" = {
          username = var.registry_username
          password = var.registry_password
          auth     = base64encode("${var.registry_username}:${var.registry_password}")
        }
      }
    })
  }

  depends_on = [kubectl_manifest.espx_namespace]
}

resource "kubectl_manifest" "espx_namespace" {
  yaml_body = replace(
    file("${local.k8s_root}/base/namespace.yaml"),
    "name: espx",
    "name: ${var.namespace}",
  )
}

resource "kubectl_manifest" "espx_configmap" {
  depends_on = [kubectl_manifest.espx_namespace]

  yaml_body = templatefile("${local.k8s_root}/base/configmap-env.staging.tpl", {
    namespace         = var.namespace
    env               = var.env
    redis_addrs       = var.redis_addrs
    filter_timeout_ms = var.filter_timeout_ms
    trusted_proxies   = var.trusted_proxies
    udp_tracker_addrs = var.udp_tracker_addrs
  })
}

resource "kubectl_manifest" "espx_secret" {
  depends_on = [kubectl_manifest.espx_namespace]

  yaml_body = templatefile("${local.k8s_root}/base/secret-env.staging.tpl", local.secret_template_vars)
}

resource "kubectl_manifest" "espx_processor" {
  depends_on = [
    kubectl_manifest.espx_configmap,
    kubectl_manifest.espx_secret,
    kubernetes_secret.registry,
  ]

  yaml_body        = local.patched_processor_yaml
  wait_for_rollout = false
}

resource "kubectl_manifest" "espx_apps" {
  for_each = toset(local.k8s_apps_static)

  depends_on = [
    kubectl_manifest.espx_configmap,
    kubectl_manifest.espx_secret,
    kubernetes_secret.registry,
  ]

  yaml_body        = local.patched_apps_yaml[each.value]
  wait_for_rollout = false
}

resource "kubectl_manifest" "espx_ingress" {
  count = var.enable_ingress ? 1 : 0

  depends_on = [
    kubectl_manifest.espx_configmap,
    kubectl_manifest.espx_secret,
  ]

  yaml_body = templatefile("${local.k8s_root}/apps/ingress-cold-path.yaml.tpl", {
    namespace            = var.namespace
    ingress_class        = var.ingress_class
    admin_host           = var.admin_host
    payment_webhook_host = var.payment_webhook_host
    tls_secret_name      = var.tls_secret_name
  })
  wait_for_rollout = false
}
