package app_services_app

import (
	"context"
	"fmt"
	"strings"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/datafy-io/terraform-provider-mongodb/internal/providerdata"
	"github.com/hashicorp/terraform-plugin-framework-validators/datasourcevalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure implementation satisfies interfaces.
var _ datasource.DataSource = &DataSource{}
var _ datasource.DataSourceWithConfigure = &DataSource{}
var _ datasource.DataSourceWithConfigValidators = &DataSource{}

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
	Product     types.String `tfsdk:"product"`
}

func (d *DataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_app_services_app"
}

func (d *DataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Looks up an Atlas App Services app by name or client App ID, for its ObjectID (`id`), which " +
			"`mongodb_app_services_function` and `mongodbatlas_event_trigger` take as `app_id`. Needs `atlas_public_key` and `atlas_private_key` in the provider block.",
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
				Optional:    true,
				Computed:    true,
				Description: "App name. Set this or `client_app_id`.",
			},
			"client_app_id": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "The client App ID shown in the Atlas UI, e.g. `triggers-abcde`. Set this or `name`.",
			},
			"product": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "The app's product type: `standard`, `atlas` (the app behind triggers made in the Atlas UI), `data-api` or `device-sync`. All types are searched when unset.",
				Validators: []validator.String{
					stringvalidator.OneOf(strings.Split(appservices.AllProducts, ",")...),
				},
			},
		},
	}
}

func (d *DataSource) ConfigValidators(_ context.Context) []datasource.ConfigValidator {
	return []datasource.ConfigValidator{
		datasourcevalidator.ExactlyOneOf(
			path.MatchRoot("name"),
			path.MatchRoot("client_app_id"),
		),
	}
}

func (d *DataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, diags := providerdata.AppServicesClient(req.ProviderData)
	resp.Diagnostics.Append(diags...)
	d.client = client
}

func (d *DataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config DataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	product := appservices.AllProducts
	if !config.Product.IsNull() {
		product = config.Product.ValueString()
	}

	lar, err := d.client.ListApps(ctx, &appservices.ListAppsRequest{
		ProjectId: config.ProjectId.ValueString(),
		Product:   product,
	})
	if err != nil {
		resp.Diagnostics.AddError("List apps failed", fmt.Sprintf("Could not list the App Services apps of project %s: %s", config.ProjectId.ValueString(), err))
		return
	}

	var matches []appservices.App
	for _, a := range lar.Apps {
		if (!config.Name.IsNull() && a.Name == config.Name.ValueString()) ||
			(!config.ClientAppId.IsNull() && a.ClientAppId == config.ClientAppId.ValueString()) {
			matches = append(matches, a)
		}
	}

	wanted := fmt.Sprintf("name %q", config.Name.ValueString())
	if !config.ClientAppId.IsNull() {
		wanted = fmt.Sprintf("client App ID %q", config.ClientAppId.ValueString())
	}
	switch {
	case len(matches) == 0:
		resp.Diagnostics.AddError("App not found", fmt.Sprintf("Project %s has no App Services app with %s (product types searched: %s).", config.ProjectId.ValueString(), wanted, product))
		return
	case len(matches) > 1:
		resp.Diagnostics.AddError("More than one app found", fmt.Sprintf("Project %s has %d App Services apps with %s; set product to choose one.", config.ProjectId.ValueString(), len(matches), wanted))
		return
	}

	config.ID = types.StringValue(matches[0].AppId)
	config.Name = types.StringValue(matches[0].Name)
	config.ClientAppId = types.StringValue(matches[0].ClientAppId)
	config.Product = types.StringValue(matches[0].Product)
	resp.Diagnostics.Append(resp.State.Set(ctx, &config)...)
}
