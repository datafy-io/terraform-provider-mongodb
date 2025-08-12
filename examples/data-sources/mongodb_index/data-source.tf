data "mongodb_index" "example" {
  database   = "example-account"
  collection = "users"
  name       = "users_email"
}
