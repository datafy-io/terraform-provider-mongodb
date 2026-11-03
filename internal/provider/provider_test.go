package provider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

var testProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"mongodb": providerserver.NewProtocol6WithError(New("test")()),
}

func functionConfig(api *fakeAdminAPI, source string) string {
	return api.providerConfig() + fmt.Sprintf(`
data "mongodb_app_services_app" "triggers" {
  project_id = %[1]q
  name       = "Triggers"
}

resource "mongodb_app_services_function" "job" {
  project_id    = %[1]q
  app_id        = data.mongodb_app_services_app.triggers.id
  name          = "agg_utilization_job_staging"
  source        = %[2]q
  private       = true
  run_as_system = true
}
`, testProjectId, source)
}

// functionInFake checks the function the state points at exists in the fake API with this source.
func functionInFake(api *fakeAdminAPI, source string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs := s.RootModule().Resources["mongodb_app_services_function.job"]
		fn, ok := api.function(rs.Primary.Attributes["app_id"], rs.Primary.Attributes["id"])
		if !ok {
			return fmt.Errorf("function %s is not in the API", rs.Primary.Attributes["id"])
		}
		if fn.Source != source {
			return fmt.Errorf("function source is %q, want %q", fn.Source, source)
		}
		return nil
	}
}

func TestFunctionResource(t *testing.T) {
	api := newFakeAdminAPI(t)
	importId := func(s *terraform.State) (string, error) {
		rs := s.RootModule().Resources["mongodb_app_services_function.job"]
		return fmt.Sprintf("%s--%s--%s", testProjectId, testAppId, rs.Primary.Attributes["id"]), nil
	}
	var firstId string

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		CheckDestroy: func(_ *terraform.State) error {
			api.mu.Lock()
			defer api.mu.Unlock()
			if len(api.functions) != 0 {
				return fmt.Errorf("%d functions left after destroy", len(api.functions))
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				// Create.
				Config: functionConfig(api, "exports = function () { return 1; };"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.mongodb_app_services_app.triggers", "id", testAppId),
					resource.TestCheckResourceAttr("data.mongodb_app_services_app.triggers", "client_app_id", "triggers-abcde"),
					resource.TestCheckResourceAttr("data.mongodb_app_services_app.triggers", "product", "atlas"),
					resource.TestCheckResourceAttr("mongodb_app_services_function.job", "app_id", testAppId),
					resource.TestCheckResourceAttrSet("mongodb_app_services_function.job", "id"),
					resource.TestCheckResourceAttrWith("mongodb_app_services_function.job", "id", func(v string) error { firstId = v; return nil }),
					functionInFake(api, "exports = function () { return 1; };"),
				),
			},
			{
				// A source change updates the function in place, keeping its ID (which triggers point at).
				Config: functionConfig(api, "exports = function () { return 2; };"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("mongodb_app_services_function.job", plancheck.ResourceActionUpdate),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttrWith("mongodb_app_services_function.job", "id", func(v string) error {
						if v != firstId {
							return fmt.Errorf("id changed from %s to %s", firstId, v)
						}
						return nil
					}),
					functionInFake(api, "exports = function () { return 2; };"),
				),
			},
			{
				// terraform import by function ID.
				ResourceName:                         "mongodb_app_services_function.job",
				ImportState:                          true,
				ImportStateIdFunc:                    importId,
				ImportStateVerify:                    true,
				ImportStateVerifyIdentifierAttribute: "id",
			},
			{
				// terraform import by function name.
				ResourceName:                         "mongodb_app_services_function.job",
				ImportState:                          true,
				ImportStateId:                        fmt.Sprintf("%s--%s--agg_utilization_job_staging", testProjectId, testAppId),
				ImportStateVerify:                    true,
				ImportStateVerifyIdentifierAttribute: "id",
			},
			{
				// A function deleted outside Terraform is planned for creation again.
				PreConfig: func() {
					api.mu.Lock()
					defer api.mu.Unlock()
					clear(api.functions)
				},
				Config: functionConfig(api, "exports = function () { return 2; };"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("mongodb_app_services_function.job", plancheck.ResourceActionCreate),
					},
				},
				Check: functionInFake(api, "exports = function () { return 2; };"),
			},
		},
	})
}

// A function made in the Atlas UI is adopted with an import block (by name), and updated in the same apply.
func TestFunctionImportBlock(t *testing.T) {
	api := newFakeAdminAPI(t)
	api.addFunction(testAppId, appservices.Function{Name: "agg_utilization_job_staging", Source: "exports = function () { return 0; };", Private: true, RunAsSystem: true})

	importBlock := fmt.Sprintf(`
import {
  to = mongodb_app_services_function.job
  id = "%s--${data.mongodb_app_services_app.triggers.id}--agg_utilization_job_staging"
}
`, testProjectId)

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				// Same settings as in the UI: imported with no change.
				Config: functionConfig(api, "exports = function () { return 0; };") + importBlock,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("mongodb_app_services_function.job", plancheck.ResourceActionNoop),
					},
				},
				Check: functionInFake(api, "exports = function () { return 0; };"),
			},
			{
				// The import block stays in place harmlessly; a new source updates the adopted function.
				Config: functionConfig(api, "exports = function () { return 1; };") + importBlock,
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{
						plancheck.ExpectResourceAction("mongodb_app_services_function.job", plancheck.ResourceActionUpdate),
					},
				},
				Check: resource.ComposeAggregateTestCheckFunc(
					functionInFake(api, "exports = function () { return 1; };"),
					func(_ *terraform.State) error {
						api.mu.Lock()
						defer api.mu.Unlock()
						if api.creates != 0 {
							return fmt.Errorf("the provider created %d functions instead of importing", api.creates)
						}
						return nil
					},
				),
			},
		},
	})
}

func TestFunctionImportErrors(t *testing.T) {
	api := newFakeAdminAPI(t)

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:        functionConfig(api, "exports = function () {};"),
				ResourceName:  "mongodb_app_services_function.job",
				ImportState:   true,
				ImportStateId: testAppId + "/agg_utilization_job_staging",
				ExpectError:   regexp.MustCompile(`Expected <project_id>--<app_id>--<function ID or name>`),
			},
			{
				Config:        functionConfig(api, "exports = function () {};"),
				ResourceName:  "mongodb_app_services_function.job",
				ImportState:   true,
				ImportStateId: fmt.Sprintf("%s--%s--no_such_function", testProjectId, testAppId),
				ExpectError:   regexp.MustCompile(`has no function with ID or name\s+"no_such_function"`),
			},
		},
	})
}

func TestAppDataSource(t *testing.T) {
	api := newFakeAdminAPI(t)
	appConfig := func(attributes string) string {
		return api.providerConfig() + fmt.Sprintf(`
data "mongodb_app_services_app" "this" {
  project_id = %q
  %s
}
`, testProjectId, attributes)
	}

	resource.UnitTest(t, resource.TestCase{
		ProtoV6ProviderFactories: testProviderFactories,
		// The invalid configs come first: the test destroys with the last step's config.
		Steps: []resource.TestStep{
			{
				Config:      appConfig("name = \"Triggers\"\n  product = \"standard\""),
				ExpectError: regexp.MustCompile(`no App Services app with name "Triggers"`),
			},
			{
				Config:      appConfig(`name = "Triggers"` + "\n  client_app_id = \"triggers-abcde\""),
				ExpectError: regexp.MustCompile(`Invalid Attribute Combination`),
			},
			{
				Config: appConfig(`client_app_id = "triggers-abcde"`),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.mongodb_app_services_app.this", "id", testAppId),
					resource.TestCheckResourceAttr("data.mongodb_app_services_app.this", "name", "Triggers"),
				),
			},
			{
				Config: appConfig(`name = "web"`),
				Check:  resource.TestCheckResourceAttr("data.mongodb_app_services_app.this", "product", "standard"),
			},
		},
	})
}
