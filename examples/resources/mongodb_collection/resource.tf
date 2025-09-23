resource "mongodb_collection" "example" {
  database = "example-account"
  name     = "users"
}
