# Triggers made in the Atlas UI live in an app of product type "atlas".
data "mongodb_app_services_app" "triggers" {
  project_id = var.atlas_project_id
  name       = "Triggers"
}
