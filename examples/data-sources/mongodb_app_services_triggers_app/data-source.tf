# The app Atlas made for the project's triggers, whatever its name.
data "mongodb_app_services_triggers_app" "this" {
  project_id = var.atlas_project_id
}
