package index

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int32planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"slices"
	"strings"
)

var _ resource.Resource = &Resource{}
var _ resource.ResourceWithConfigure = &Resource{}
var _ resource.ResourceWithImportState = &Resource{}

func NewResource() resource.Resource { return &Resource{} }

type Resource struct {
	client *mongo.Client
}

type indexKeyModel struct {
	Field types.String `tfsdk:"field"`
	Order types.Int64  `tfsdk:"order"`
}

type ResourceModel struct {
	ID         types.String    `tfsdk:"id"`
	Database   types.String    `tfsdk:"database"`
	Collection types.String    `tfsdk:"collection"`
	Name       types.String    `tfsdk:"name"`
	Unique     types.Bool      `tfsdk:"unique"`
	Sparse     types.Bool      `tfsdk:"sparse"`
	TTL        types.Int32     `tfsdk:"ttl"`
	Partial    types.String    `tfsdk:"partial_filter_expression"`
	Background types.Bool      `tfsdk:"background"`
	Keys       []indexKeyModel `tfsdk:"keys"`
}

func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_index"
}

func (r *Resource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"database": schema.StringAttribute{
				Required:    true,
				Description: "Database name.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"collection": schema.StringAttribute{
				Required:    true,
				Description: "Collection name.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Index name. If not specified, MongoDB will generate a name based on the indexed fields.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"unique": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "If true, the index enforces a uniqueness constraint on the indexed field(s).",
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"sparse": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "If true, the index only includes documents that contain the indexed field.",
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"ttl": schema.Int32Attribute{
				Optional:    true,
				Description: "Time-to-live in seconds for the index. When specified, MongoDB will automatically delete documents when their indexed field value is older than the specified TTL.",
				PlanModifiers: []planmodifier.Int32{
					int32planmodifier.RequiresReplace(),
				},
			},
			"background": schema.BoolAttribute{
				Optional:           true,
				Computed:           true,
				Default:            booldefault.StaticBool(true),
				Description:        "If true, the index is built in the background. (Default: true)",
				DeprecationMessage: "Background index builds are deprecated in MongoDB 4.2 and later.",
			},
			"partial_filter_expression": schema.StringAttribute{
				Optional:    true,
				Description: "JSON string for partial filter expression.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
		Blocks: map[string]schema.Block{
			"keys": schema.ListNestedBlock{
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"field": schema.StringAttribute{
							Required: true,
							PlanModifiers: []planmodifier.String{
								stringplanmodifier.RequiresReplace(),
							},
						},
						"order": schema.Int64Attribute{
							Required: true,
							PlanModifiers: []planmodifier.Int64{
								int64planmodifier.RequiresReplace(),
							},
						},
					}},
				Validators: []validator.List{
					listvalidator.SizeAtLeast(1),
				},
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *Resource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*mongo.Client)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *mongo.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
}

func (r *Resource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	indexes := r.client.Database(plan.Database.ValueString()).Collection(plan.Collection.ValueString()).Indexes()

	specifications, err := indexes.ListSpecifications(ctx)
	if err != nil {
		resp.Diagnostics.AddError("List indexes failed", err.Error())
		return
	}

	if slices.ContainsFunc(specifications, func(specification *mongo.IndexSpecification) bool {
		return specification.Name == plan.Name.ValueString()
	}) {
		resp.Diagnostics.AddError(
			"Index already exists",
			fmt.Sprintf("An index named %s already exists.", plan.Name.ValueString()),
		)
		return
	}

	keys := bson.D{}
	for _, k := range plan.Keys {
		keys = append(keys, bson.E{Key: k.Field.ValueString(), Value: int(k.Order.ValueInt64())})
	}

	idx := mongo.IndexModel{
		Keys:    keys,
		Options: &options.IndexOptions{},
	}

	idx.Options.Unique = plan.Unique.ValueBoolPointer()
	idx.Options.Sparse = plan.Sparse.ValueBoolPointer()
	idx.Options.ExpireAfterSeconds = plan.TTL.ValueInt32Pointer()
	idx.Options.Name = plan.Name.ValueStringPointer()
	idx.Options.Background = plan.Background.ValueBoolPointer()

	if p := plan.Partial.ValueString(); p != "" {
		var raw bson.Raw
		if err := bson.UnmarshalExtJSON([]byte(p), true, &raw); err != nil {
			resp.Diagnostics.AddError("invalid partial_filter_expression JSON", err.Error())
			return
		}
		idx.Options.PartialFilterExpression = raw
	}

	name, err := indexes.CreateOne(ctx, idx)
	if err != nil {
		resp.Diagnostics.AddError("create index failed", err.Error())
		return
	}

	plan.ID = types.StringValue(fmt.Sprintf("%s/%s/%s", plan.Database.ValueString(), plan.Collection.ValueString(), name))
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	indexes, err := r.client.Database(state.Database.ValueString()).Collection(state.Collection.ValueString()).Indexes().ListSpecifications(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Failed to list index", err.Error())
		return
	}

	if !slices.ContainsFunc(indexes, func(index *mongo.IndexSpecification) bool {
		return index.Name == state.Name.ValueString()
	}) {
		resp.State.RemoveResource(ctx)
	}

	var index *mongo.IndexSpecification
	for _, i := range indexes {
		if i != nil && i.Name == state.Name.ValueString() {
			index = i
			break
		}
	}
	if index == nil {
		resp.Diagnostics.AddError("Index not found", "")
		return
	}

	state.Unique = types.BoolPointerValue(index.Unique)
	state.Sparse = types.BoolPointerValue(index.Sparse)
	state.TTL = types.Int32PointerValue(index.ExpireAfterSeconds)

	var keysDoc bson.D
	if err := bson.Unmarshal(index.KeysDocument, &keysDoc); err != nil {
		resp.Diagnostics.AddError("Failed to decode index keys", err.Error())
		return
	}
	state.Keys = make([]indexKeyModel, 0, len(keysDoc))
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
		state.Keys = append(state.Keys, indexKeyModel{
			Field: types.StringValue(e.Key),
			Order: types.Int64Value(order),
		})
	}

	state.ID = types.StringValue(fmt.Sprintf("%s/%s/%s", state.Database.ValueString(), state.Collection.ValueString(), state.Name.ValueString()))
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// All meaningful changes are ForceNew semantics; just keep state
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if _, err := r.client.Database(state.Database.ValueString()).Collection(state.Collection.ValueString()).Indexes().DropOne(ctx, state.Name.ValueString()); err != nil {
		resp.Diagnostics.AddError("drop index failed", err.Error())
	}
}

func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := strings.TrimSpace(req.ID)
	if id == "" {
		resp.Diagnostics.AddError(
			"Empty import ID",
			"Expected format: 'database/collection/index'",
		)
		return
	}

	parts := strings.SplitN(id, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		resp.Diagnostics.AddError(
			"Invalid import ID",
			fmt.Sprintf("Expected 'database/collection/index', got %s", id),
		)
		return
	}
	db, coll, index := parts[0], parts[1], parts[2]

	var state ResourceModel
	state.ID = types.StringValue(id)
	state.Name = types.StringValue(index)
	state.Collection = types.StringValue(coll)
	state.Database = types.StringValue(db)

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
