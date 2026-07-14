terraform {
  required_version = ">= 1.5.0"

  required_providers {
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.35"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.1"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.4"
    }
  }
}
