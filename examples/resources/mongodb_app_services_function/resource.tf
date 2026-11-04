data "mongodb_app_services_triggers_app" "this" {
  project_id = var.atlas_project_id
}

resource "mongodb_app_services_function" "job" {
  project_id    = var.atlas_project_id
  app_id        = data.mongodb_app_services_triggers_app.this.id
  name          = "rollup_job"
  source        = file("${path.module}/functions/rollup_job.js")
  private       = true
  run_as_system = true
}

# Run it every 5 minutes.
resource "mongodbatlas_event_trigger" "job" {
  project_id      = var.atlas_project_id
  app_id          = data.mongodb_app_services_triggers_app.this.id
  name            = "rollup_job"
  type            = "SCHEDULED"
  function_id     = mongodb_app_services_function.job.id
  config_schedule = "*/5 * * * *"
}
