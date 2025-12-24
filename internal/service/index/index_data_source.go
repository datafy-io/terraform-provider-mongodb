package index

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-jsontypes/jsontypes"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ datasource.DataSource = &DataSource{}
var _ datasource.DataSourceWithConfigure = &DataSource{}

func NewDataSource() datasource.DataSource {
	return &DataSource{}
}

type DataSource struct {
	client *mongo.Client
}

type DataSourceModel struct {
	ID         types.String         `tfsdk:"id"`
	Database   types.String         `tfsdk:"database"`
	Collection types.String         `tfsdk:"collection"`
	Name       types.String         `tfsdk:"name"`
	Unique     types.Bool           `tfsdk:"unique"`
	Sparse     types.Bool           `tfsdk:"sparse"`
	TTL        types.Int32          `tfsdk:"ttl"`
	Partial    jsontypes.Normalized `tfsdk:"partial_filter_expression"`
	Keys       []indexKeyModel      `tfsdk:"keys"`
}

func (d *DataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_index"
}

func (d *DataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Retrieves a specific Datafy account.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
			},
			"database": schema.StringAttribute{
				Required:    true,
				Description: "Database name.",
			},
			"collection": schema.StringAttribute{
				Required:    true,
				Description: "Collection name.",
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Index name. If not specified, MongoDB will generate a name based on the indexed fields.",
			},
			"unique": schema.BoolAttribute{
				Computed:    true,
				Description: "If true, the index enforces a uniqueness constraint on the indexed field(s).",
			},
			"sparse": schema.BoolAttribute{
				Computed:    true,
				Description: "If true, the index only includes documents that have the indexed field(s).",
			},
			"ttl": schema.Int32Attribute{
				Computed:    true,
				Description: "Time-to-live in seconds for the index. When specified, MongoDB will automatically delete documents when their indexed field value is older than the specified TTL.",
			},
			"partial_filter_expression": schema.StringAttribute{
				CustomType:  jsontypes.NormalizedType{},
				Computed:    true,
				Description: "JSON string for partial filter expression.",
			},
		},
		Blocks: map[string]schema.Block{
			"keys": schema.ListNestedBlock{
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"field": schema.StringAttribute{
							Computed: true,
						},
						"order": schema.Int64Attribute{
							Computed: true,
						},
					}},
			},
		},
	}
}

func (d *DataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*mongo.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected DataSource Configure Type",
			fmt.Sprintf("Expected *mongo.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	d.client = client
}

func (d *DataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var plan DataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	indexes, err := ExIndexView{d.client.Database(plan.Database.ValueString()).Collection(plan.Collection.ValueString()).Indexes()}.ListExSpecifications(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Failed to list index specifications", err.Error())
		return
	}

	index := indexes.Find(plan.Name.ValueString())
	if index == nil {
		resp.Diagnostics.AddError("Index not found", "")
		return
	}

	plan.Sparse = types.BoolPointerValue(index.Sparse)
	plan.Unique = types.BoolPointerValue(index.Unique)
	plan.TTL = types.Int32PointerValue(index.ExpireAfterSeconds)
	if len(index.PartialFilterExpression) > 0 {
		extJSON, err := bson.MarshalExtJSON(index.PartialFilterExpression, true, true)
		if err != nil {
			resp.Diagnostics.AddError("Failed to marshal partial filter expression", err.Error())
			return
		}
		plan.Partial = jsontypes.NewNormalizedValue(string(extJSON))
	}

	var keysDoc bson.D
	if err := bson.Unmarshal(index.KeysDocument, &keysDoc); err != nil {
		resp.Diagnostics.AddError("Failed to decode index keys", err.Error())
		return
	}
	plan.Keys = make([]indexKeyModel, 0, len(keysDoc))
	for _, e := range keysDoc {
		var order int64
		switch v := e.Value.(type) {
		case int32:
			order = int64(v)
		case int64:
			order = v
		case float64:
			// allow 1.0 / -1.0 coming back as doubles
			order = int64(v)
		default:
			// unsupported (e.g., "2dsphere", "text")
			resp.Diagnostics.AddWarning(
				"Non-numeric index key order encountered",
				fmt.Sprintf("Field %q has unsupported type %T (value %v). Skipping.", e.Key, v, v),
			)
			continue
		}
		plan.Keys = append(plan.Keys, indexKeyModel{
			Field: types.StringValue(e.Key),
			Order: types.Int64Value(order),
		})
	}

	plan.ID = types.StringValue(fmt.Sprintf("%s/%s/%s", plan.Database.ValueString(), plan.Collection.ValueString(), plan.Name.ValueString()))
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}
