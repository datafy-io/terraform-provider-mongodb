# terraform-provider-mongodb

Manages MongoDB databases, collections and indexes over a MongoDB connection (`uri`), and Atlas App Services Functions
over the [App Services Admin API](https://www.mongodb.com/docs/atlas/app-services/admin/api/v3/) (`atlas_public_key` and `atlas_private_key`).
Set either or both: without `uri` the provider makes no MongoDB connection, so a configuration with only App Services resources
doesn't need network access to the cluster.

## App Services Functions

`mongodb_app_services_function` manages a Function, and `mongodb_app_services_app` looks up an app's ID. The official
`mongodb/mongodbatlas` provider has triggers (`mongodbatlas_event_trigger`) but no Functions; pass it the function's `id`.
The Atlas API key needs Project Owner on the project to make changes.

```hcl
data "mongodb_app_services_app" "triggers" {
  project_id = var.atlas_project_id
  name       = "Triggers"
}

resource "mongodb_app_services_function" "job" {
  project_id    = var.atlas_project_id
  app_id        = data.mongodb_app_services_app.triggers.id
  name          = "rollup_job"
  source        = file("${path.module}/functions/rollup_job.js")
  private       = true
  run_as_system = true
}

resource "mongodbatlas_event_trigger" "job" {
  project_id      = var.atlas_project_id
  app_id          = data.mongodb_app_services_app.triggers.id
  name            = "rollup_job"
  type            = "SCHEDULED"
  function_id     = mongodb_app_services_function.job.id
  config_schedule = "*/5 * * * *"
}
```

The import ID is `<project_id>--<app_id>--<function>`, where `<function>` is the function's ID or its name, so a function made in the
Atlas UI can be imported by name. It uses the same separator as `mongodbatlas_event_trigger` (`<project_id>--<app_id>--<trigger_id>`).

```hcl
import {
  to = mongodb_app_services_function.job
  id = "${var.atlas_project_id}--${data.mongodb_app_services_app.triggers.id}--rollup_job"
}
```

- An update replaces the whole function: `can_evaluate`, `run_as_user_id` and `run_as_user_id_script_source` are not managed and are cleared.
- Once a function is in Terraform, edit it here, not in the Atlas UI: the next apply puts back what the configuration says.

## Development

```shell
make test      # unit tests, and tests that run terraform against a fake App Services Admin API
make generate  # format examples/ and regenerate docs/
```
