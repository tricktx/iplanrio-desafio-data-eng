#############################
# VARIABLES
#############################

variable "project_name" {
    description = "Nome do projeto (display name)"
    type        = string
    default     = "br-cgu-terceirizados"
}

###########
# O ID do Projeto deve ser globalmente único.
# Referência: https://cloud.google.com/resource-manager/docs/creating-managing-projects?hl=pt-br
##########
variable "project_id" {
    description = "ID do projeto no GCP"
    type        = string
    default     = "br-cgu-terceirizados"
}


variable "service_account_adm_dados" {
    description = "Administrador de dados - Garante acesso como editor e leitor do Storage"
    type        = string
    default     = "chave-adm-dados"
}

variable "billing_account" {
    description = "ID da conta de faturamento (ex: XXXXXX-XXXXXX-XXXXXX)"
    type        = string
}


###########
# O nome do bucket deve ser globalmente único.
# Referência: https://cloud.google.com/storage/docs/buckets?hl=pt-br
##########
variable "bucket_name" {
    description = "Nome do bucket"
    type        = string
    default     = "br-cgu-terceirizados"
}

variable "location" {
    description = "Localização do bucket"
    type        = string
    default     = "southamerica-east1"
}

variable "storage_class" {
    description = "Classe de armazenamento"
    type        = string
    default     = "STANDARD"
}

variable "zone" {
    description     = "A região dos recursos do Google Cloud"
    type            = string
    default         = "southamerica-east1-a"
    }