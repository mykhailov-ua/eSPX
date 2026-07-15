terraform {
  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

# Idempotent k3s install via scripts/k8s/install_k3s.sh.
# Re-runs when install script checksum or kubeconfig path changes.
resource "null_resource" "k3s_install" {
  triggers = {
    install_script = filemd5("${path.module}/../../../../scripts/k8s/install_k3s.sh")
    kubeconfig     = var.kubeconfig_path
  }

  provisioner "local-exec" {
    command     = "bash ${abspath(path.module)}/../../../../scripts/k8s/install_k3s.sh --kubeconfig ${var.kubeconfig_path}"
    working_dir = abspath(path.module)
  }
}

# Gate for downstream kubectl provider: kubeconfig file must exist after bootstrap.
data "external" "k3s_ready" {
  depends_on = [null_resource.k3s_install]

  program = ["bash", "-c", <<-EOT
    if [ -f "${var.kubeconfig_path}" ]; then
      echo '{"ready":"true"}'
    else
      echo '{"ready":"false"}'
      exit 1
    fi
  EOT
  ]
}
