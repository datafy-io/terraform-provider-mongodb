package provider

import (
	"context"
	"strings"
	"time"

	"github.com/datafy-io/terraform-provider-mongodb/internal/service/collection"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/database"
	"github.com/datafy-io/terraform-provider-mongodb/internal/service/index"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Ensure the implementation satisfies the expected interfaces.
var _ provider.Provider = &mongodbProvider{}

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
}

type providerData struct {
	Client *mongo.Client
}

func (p *mongodbProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "mongodb"
	resp.Version = p.version
}

func (p *mongodbProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"uri": schema.StringAttribute{
				Required:    true,
				Description: "MongoDB URI, e.g. mongodb+srv://cluster0.x.mongodb.net",
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
	}
}

func (p *mongodbProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config providerModel

	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	uri := config.URI.ValueString()
	user := config.Username.ValueString()
	pass := config.Password.ValueString()

	// In Configure
	if uri == "" {
		resp.Diagnostics.AddError("Missing URI", "The 'uri' attribute is required")
		return
	}
	if (user != "" || pass != "") && strings.Contains(uri, "@") {
		resp.Diagnostics.AddError("Invalid Credentials Setup", "When username/password are provided, SRV must not contain userinfo")
		return
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
		return
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		resp.Diagnostics.AddError("Mongo ping failed", err.Error())
		return
	}

	resp.ResourceData = client
	resp.DataSourceData = client
}

func (p *mongodbProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		database.NewResource,
		collection.NewResource,
		index.NewResource,
	}
}

func (p *mongodbProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		database.NewDataSource,
		collection.NewDataSource,
		index.NewDataSource,
	}
}
