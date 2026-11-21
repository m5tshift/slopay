terraform {
  required_version = ">= 1.9"

  required_providers {
    hyperv = {
      source  = "taliesins/hyperv"
      version = "1.2.1"
    }
  }
}
