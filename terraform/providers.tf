terraform {
    required_providers {
        google = {
                source  = "hashicorp/google" # 	Especifica o endereço de origem global do provedor.
                version = "6.36.1" # 	Especifica a versão do provedor que essa configuração deve usar.
        }
    }
}

provider "google" {
    region                  = var.location
    zone                    = var.zone
}