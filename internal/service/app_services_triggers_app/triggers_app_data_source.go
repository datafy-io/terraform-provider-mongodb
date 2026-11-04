package app_services_triggers_app

import (
	"context"
	"fmt"
	"strings"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/datafy-io/terraform-provider-mongodb/internal/providerdata"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure implementation satisfies interfaces.
var _ datasource.DataSource = &DataSource{}
var _ datasource.DataSourceWithConfigure = &DataSource{}

// triggersProduct is the product type of the app Atlas creates for its triggers. The API lists apps of this type
// only when asked for it.
const triggersProduct = "atlas"

func NewDataSource() datasource.DataSource {
	return &DataSource{}
}

type DataSource struct {
	client *appservices.Client
}

type DataSourceModel struct {
	ID          types.String `tfsdk:"id"`
	ProjectId   types.String `tfsdk:"project_id"`
	Name        types.String `tfsdk:"name"`
	ClientAppId types.String `tfsdk:"client_app_id"`
}

func (d *DataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_app_services_triggers_app"
}

func (d *DataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up the project's Atlas Triggers app: the App Services app (product type `atlas`) that Atlas creates " +
			"the first time a trigger is made in the Atlas UI, and that holds the project's triggers and their functions. " +
			"Its ObjectID (`id`) is what `mongodb_app_services_function` and `mongodbatlas_event_trigger` take as `app_id`. " +
			"Needs the provider's `atlas` block.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The app's ObjectID.",
			},
			"project_id": schema.StringAttribute{
				Required:    true,
				Description: "The Atlas project ID.",
			},
			"name": schema.StringAttribute{
				Computed:    true,
				Description: "The app's name, chosen by Atlas: `Triggers`, or `Triggers_RealmApp` or `Triggers_StitchApp` in older projects.",
			},
			"client_app_id": schema.StringAttribute{
				Computed:    true,
				Description: "The client App ID shown in the Atlas UI, e.g. `triggers-abcde`.",
			},
		},
	}
}

func (d *DataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, diags := providerdata.AppServicesClient(req.ProviderData)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	d.client = client
}

func (d *DataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config DataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	projectId := config.ProjectId.ValueString()
	lar, err := d.client.ListApps(ctx, &appservices.ListAppsRequest{
		ProjectId: projectId,
		Product:   triggersProduct,
	})
	if err != nil {
		resp.Diagnostics.AddError("List apps failed", fmt.Sprintf("Could not list the App Services apps of project %s: %s", projectId, err))
		return
	}

	switch {
	case len(lar.Apps) == 0:
		resp.Diagnostics.AddError("No Atlas Triggers app",
			fmt.Sprintf("Project %s has no App Services app of product type %q. Atlas creates it the first time a trigger is made "+
				"in the Atlas UI (Triggers page); make one trigger there once.", projectId, triggersProduct))
		return
	case len(lar.Apps) > 1:
		apps := make([]string, 0, len(lar.Apps))
		for _, a := range lar.Apps {
			apps = append(apps, fmt.Sprintf("%s (%s)", a.Name, a.ClientAppId))
		}
		resp.Diagnostics.AddError("More than one Atlas Triggers app",
			fmt.Sprintf("Project %s has %d App Services apps of product type %q, expected one: %s.", projectId, len(lar.Apps), triggersProduct, strings.Join(apps, ", ")))
		return
	}

	app := lar.Apps[0]
	config.ID = types.StringValue(app.AppId)
	config.Name = types.StringValue(app.Name)
	config.ClientAppId = types.StringValue(app.ClientAppId)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}
