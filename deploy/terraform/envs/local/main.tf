locals {
  kubeconfig = pathexpand(var.kubeconfig_path)
  k8s_root   = abspath("${path.module}/${var.k8s_manifests_path}")

  k8s_apps_static = [
    for f in fileset("${local.k8s_root}/apps", "*.yaml") :
    "${local.k8s_root}/apps/${f}"
    if !endswith(f, "kustomization.yaml")
  ]

  host_ip         = data.external.node_internal_ip.result.ip
  geoip_host_path = abspath(var.geoip_host_path)
}

module "k3s" {
  source = "../../modules/k3s-install"

  kubeconfig_path = local.kubeconfig
}

data "external" "node_internal_ip" {
  depends_on = [module.k3s]

  program = ["bash", "-c", <<-EOT
    ip=$(kubectl --kubeconfig ${local.kubeconfig} get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    if [ -z "$ip" ]; then
      echo '{"ip":""}' >&2
      exit 1
    fi
    echo "{\"ip\":\"$ip\"}"
  EOT
  ]
}

provider "kubernetes" {
  config_path = local.kubeconfig
}

provider "kubectl" {
  config_path = local.kubeconfig
}

resource "kubectl_manifest" "espx_namespace" {
  depends_on = [module.k3s]

  yaml_body = file("${local.k8s_root}/base/namespace.yaml")
}

resource "kubectl_manifest" "espx_configmap" {
  depends_on = [kubectl_manifest.espx_namespace]

  yaml_body = templatefile("${local.k8s_root}/base/configmap-env.yaml.tpl", {
    host_ip = local.host_ip
  })
}

resource "kubectl_manifest" "espx_secret" {
  depends_on = [kubectl_manifest.espx_namespace]

  yaml_body = templatefile("${local.k8s_root}/base/secret-env.yaml.tpl", {
    host_ip = local.host_ip
  })
}

resource "kubectl_manifest" "espx_processor" {
  depends_on = [
    kubectl_manifest.espx_configmap,
    kubectl_manifest.espx_secret,
  ]

  yaml_body = templatefile("${local.k8s_root}/apps/deployment-processor.yaml.tpl", {
    geoip_host_path = local.geoip_host_path
  })
  wait_for_rollout = false
}

resource "kubectl_manifest" "espx_apps" {
  for_each = toset(local.k8s_apps_static)

  depends_on = [
    kubectl_manifest.espx_configmap,
    kubectl_manifest.espx_secret,
  ]

  yaml_body        = file(each.value)
  wait_for_rollout = false
}
