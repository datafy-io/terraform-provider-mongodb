package provider

import (
	"context"
	"strings"
	"time"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/datafy-io/terraform-provider-mongodb/internal/providerdata"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/app_services_function"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/app_services_triggers_app"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/collection"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/database"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/index"
	"github.com/hashicorp/terraform-plugin-framework-validators/providervalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Ensure the implementation satisfies the expected interfaces.
var _ provider.Provider = &mongodbProvider{}
var _ provider.ProviderWithConfigValidators = &mongodbProvider{}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &mongodbProvider{
			version: version,
		}
	}
}

type mongodbProvider struct {
	version string
}

type providerModel struct {
	URI      types.String `tfsdk:"uri"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
	Atlas    *atlasModel  `tfsdk:"atlas"`
}

type atlasModel struct {
	PublicKey  types.String `tfsdk:"public_key"`
	PrivateKey types.String `tfsdk:"private_key"`
	BaseURL    types.String `tfsdk:"base_url"`
}

func (p *mongodbProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "mongodb"
	resp.Version = p.version
}

func (p *mongodbProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"uri": schema.StringAttribute{
				Optional:    true,
				Description: "MongoDB URI, e.g. mongodb+srv://cluster0.x.mongodb.net. Required for databases, collections and indexes.",
			},
			"username": schema.StringAttribute{
				Optional:    true,
				Description: "Username; if set, SRV must not contain userinfo.",
			},
			"password": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "Password; if set, SRV must not contain userinfo.",
			},
		},
		Blocks: map[string]schema.Block{
			"atlas": schema.SingleNestedBlock{
				Description: "An Atlas programmatic API key with Project Owner on the project (App Services needs it to make changes). " +
					"Required for App Services apps and functions.",
				Attributes: map[string]schema.Attribute{
					"public_key": schema.StringAttribute{
						Required:    true,
						Description: "Public key of the Atlas API key.",
					},
					"private_key": schema.StringAttribute{
						Required:    true,
						Sensitive:   true,
						Description: "Private key of the Atlas API key.",
					},
					"base_url": schema.StringAttribute{
						Optional:    true,
						Description: "App Services Admin API base URL. (Default: " + appservices.DefaultBaseURL + ")",
					},
				},
			},
		},
	}
}

func (p *mongodbProvider) ConfigValidators(_ context.Context) []provider.ConfigValidator {
	return []provider.ConfigValidator{
		providervalidator.AtLeastOneOf(path.MatchRoot("uri"), path.MatchRoot("atlas")),
	}
}

func (p *mongodbProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config providerModel

	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	data := &providerdata.Data{}

	// Only with a uri: the App Services resources need no connection to the cluster.
	if uri := config.URI.ValueString(); uri != "" {
		client, ok := connect(ctx, config, resp)
		if !ok {
			return
		}
		data.Mongo = client
	}

	if config.Atlas != nil {
		baseURL := config.Atlas.BaseURL.ValueString()
		if baseURL == "" {
			baseURL = appservices.DefaultBaseURL
		}
		// The client logs in on its first call.
		data.AppServices = appservices.NewClient(config.Atlas.PublicKey.ValueString(), config.Atlas.PrivateKey.ValueString(), baseURL)
	}

	resp.ResourceData = data
	resp.DataSourceData = data
}

func connect(ctx context.Context, config providerModel, resp *provider.ConfigureResponse) (*mongo.Client, bool) {
	uri := config.URI.ValueString()
	user := config.Username.ValueString()
	pass := config.Password.ValueString()

	if (user != "" || pass != "") && strings.Contains(uri, "@") {
		resp.Diagnostics.AddError("Invalid Credentials Setup", "When username/password are provided, SRV must not contain userinfo")
		return nil, false
	}

	clientOpts := options.Client().ApplyURI(uri)
	if user != "" || pass != "" {
		clientOpts.SetAuth(options.Credential{
			Username: user,
			Password: pass,
		})
	}
	clientOpts.SetServerSelectionTimeout(10 * time.Second)
	clientOpts.SetConnectTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		resp.Diagnostics.AddError("Mongo connect failed", err.Error())
		return nil, false
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		resp.Diagnostics.AddError("Mongo ping failed", err.Error())
		return nil, false
	}

	return client, true
}

func (p *mongodbProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		database.NewResource,
		collection.NewResource,
		index.NewResource,
		app_services_function.NewResource,
	}
}

func (p *mongodbProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		database.NewDataSource,
		collection.NewDataSource,
		index.NewDataSource,
		app_services_triggers_app.NewDataSource,
	}
}
