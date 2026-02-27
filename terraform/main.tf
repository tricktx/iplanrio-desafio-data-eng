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
