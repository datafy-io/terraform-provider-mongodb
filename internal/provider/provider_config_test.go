package provider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestProviderNeedsUriOrAtlasKey(t *testing.T) {
	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `
provider "mongodb" {}

data "mongodb_app_services_app" "this" {
  project_id = "p"
  name       = "Triggers"
}
`,
				ExpectError: regexp.MustCompile(`(?s)uri.*atlas_public_key`),
			},
			{
				Config: `
provider "mongodb" {
  atlas_public_key = "public"
}

data "mongodb_app_services_app" "this" {
  project_id = "p"
  name       = "Triggers"
}
`,
				ExpectError: regexp.MustCompile(`atlas_private_key`),
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

func TestFunctionPreventDestroy(t *testing.T) {
	api := newFakeAdminAPI(t)
	protected := func(preventDestroy bool) string {
		return api.providerConfig() + fmt.Sprintf(`
resource "mongodb_app_services_function" "job" {
  project_id      = %q
  app_id          = %q
  name            = "agg_utilization_job_production"
  source          = "exports = function () {};"
  private         = true
  prevent_destroy = %t
}
`, testProjectId, testAppId, preventDestroy)
	}

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: protected(true),
				Check:  resource.TestCheckResourceAttr("mongodb_app_services_function.job", "prevent_destroy", "true"),
			},
			{
				Config:      protected(true),
				Destroy:     true,
				ExpectError: regexp.MustCompile(`Prevented Function Deletion`),
			},
			{
				// Still there; lifting the flag lets the test's final destroy go through.
				Config: protected(false),
				Check: func(s *terraform.State) error {
					rs := s.RootModule().Resources["mongodb_app_services_function.job"]
					if _, ok := api.function(testAppId, rs.Primary.Attributes["id"]); !ok {
						return fmt.Errorf("the protected function was deleted")
					}
					return nil
				},
			},
		},
	})
}
