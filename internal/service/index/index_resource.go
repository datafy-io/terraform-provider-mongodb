package index

import (
	"context"
	"fmt"
	"slices"
	"strings"

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
)

var _ resource.Resource = &Resource{}
var _ resource.ResourceWithConfigure = &Resource{}

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
				Description: "If true, the index enforces a uniqueness constraint on the indexed field(s).",
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
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
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

	keys := bson.D{}
	for _, k := range plan.Keys {
		keys = append(keys, bson.E{Key: k.Field.ValueString(), Value: int(k.Order.ValueInt64())})
	}

	idx := mongo.IndexModel{
		Keys:    keys,
		Options: &options.IndexOptions{},
	}

	idx.Options.Unique = plan.Unique.ValueBoolPointer()
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

	iv := r.client.Database(plan.Database.ValueString()).Collection(plan.Collection.ValueString()).Indexes()
	name, err := iv.CreateOne(ctx, idx)
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

	specifications, err := r.client.Database(state.Database.ValueString()).Collection(state.Collection.ValueString()).Indexes().ListSpecifications(ctx)
	if err != nil {
		resp.State.RemoveResource(ctx)
		return
	}

	if !slices.ContainsFunc(specifications, func(specification *mongo.IndexSpecification) bool {
		return specification.Name == state.Name.ValueString()
	}) {
		resp.State.RemoveResource(ctx)
	}
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

func deriveIndexName(keys bson.D) string {
	segs := make([]string, 0, len(keys)*2)
	for _, e := range keys {
		segs = append(segs, fmt.Sprintf("%s_%v", e.Key, e.Value))
	}
	return strings.Join(segs, "_")
}
