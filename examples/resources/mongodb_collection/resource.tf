resource "mongodb_collection" "example" {
  database = "example-account"
  name     = "users"

  validator = jsonencode({
    bsonType = "object"
    properties = {
      email = {
        bsonType = "string"
      }
    }
  })

  validation_level  = "strict"
  validation_action = "error"
}
