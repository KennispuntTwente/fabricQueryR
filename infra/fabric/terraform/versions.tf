terraform {
  required_version = ">= 1.11.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.12"
    }
  }
}

provider "fabric" {}