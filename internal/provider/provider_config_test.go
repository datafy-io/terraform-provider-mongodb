package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestProviderNeedsUriOrAtlasKey(t *testing.T) {
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `
provider "mongodb" {}

data "mongodb_app_services_triggers_app" "this" {
  project_id = "p"
}
`,
				ExpectError: regexp.MustCompile(`(?s)uri.*atlas`),
			},
			{
				Config: `
provider "mongodb" {
  atlas {
    public_key = "public"
  }
}

data "mongodb_app_services_triggers_app" "this" {
  project_id = "p"
}
`,
				ExpectError: regexp.MustCompile(`private_key`),
			},
			{
				Config: `
provider "mongodb" {
  atlas {}
}

data "mongodb_app_services_triggers_app" "this" {
  project_id = "p"
}
`,
				ExpectError: regexp.MustCompile(`(?s)public_key.*private_key`),
			},
		},
	})
}

// The configuration from before App Services: only uri, no atlas block. The unparseable uri fails fast, in Configure,
// so getting "Mongo connect failed" means the configuration passed validation.
func TestProviderWithOnlyUri(t *testing.T) {
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `
provider "mongodb" {
  uri      = "not-a-mongodb-uri"
  username = "user"
  password = "password"
}

resource "mongodb_database" "this" {
  name = "utilization"
}
`,
				ExpectError: regexp.MustCompile(`Mongo connect failed`),
			},
		},
	})
}

// With only the Atlas API key, the provider makes no MongoDB connection; databases, collections and indexes say why they can't work.
func TestMongoResourceWithoutUri(t *testing.T) {
	api := newFakeAdminAPI(t)

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: api.providerConfig() + `
resource "mongodb_database" "this" {
  name = "utilization"
}
`,
				ExpectError: regexp.MustCompile(`Missing MongoDB URI`),
			},
		},
	})
}
