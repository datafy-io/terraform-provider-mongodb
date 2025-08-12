resource "mongodb_index" "example" {
  database   = "example-account"
  collection = "users"
  name       = "users_email"

  keys {
    field = "email"
    order = 1
  }

  unique = true
  ttl    = 300
}
