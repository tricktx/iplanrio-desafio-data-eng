#############################
# PROJETO
#############################
resource "google_project" "project" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account =  var.billing_account
  deletion_policy = "DELETE"
}


#############################
# STORAGE BUCKET
#############################
resource "google_storage_bucket" "bucket" {
    name          = var.bucket_name
    location      = var.location
    storage_class = var.storage_class
    project       = google_project.project.project_id
}


# #############################
# # SERVICE ACCOUNT - ADMINISTRADORES DE DADOS
# #############################
resource "google_service_account" "service_account_administradores_dados" {
    account_id   = "${var.service_account_adm_dados}"
    display_name = "${var.service_account_adm_dados}"
    project      = google_project.project.project_id
}

# #############################
# # IAM NA SERVICE ACCOUNT - ADMINISTRADORES DE DADOS
# #############################


# resource "google_project_iam_member" "admin_dados_storage" {
#   project = google_project.project.project_id
#   role    = "roles/storage.admin"
#   member  = "group:engenharia.dados@prefeitura.rio"
# }
