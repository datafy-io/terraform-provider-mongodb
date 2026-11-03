provider "mongodb" {
  uri      = "mongodb+srv://cluster0.x.mongodb.net"
  username = "your_username_here" # Replace with your actual username
  password = "your_password_here" # Replace with your actual password

  # Only for App Services apps and functions: an Atlas programmatic API key with Project Owner on the project.
  atlas_public_key  = var.atlas_public_key
  atlas_private_key = var.atlas_private_key
}
