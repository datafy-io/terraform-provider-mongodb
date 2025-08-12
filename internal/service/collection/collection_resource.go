package collection

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ resource.Resource = &Resource{}
var _ resource.ResourceWithConfigure = &Resource{}

func NewResource() resource.Resource {
	return &Resource{}
}

type Resource struct {
	client *mongo.Client
}

type ResourceModel struct {
	ID               types.String `tfsdk:"id"`
	Database         types.String `tfsdk:"database"`
	Name             types.String `tfsdk:"name"`
	Validator        types.String `tfsdk:"validator"`
	ValidationLevel  types.String `tfsdk:"validation_level"`
	ValidationAction types.String `tfsdk:"validation_action"`
}

func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_collection"
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
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Collection name.",
			},
			"validator": schema.StringAttribute{
				Optional:    true,
				Description: "JSON string for validator (without the $jsonSchema prefix).",
			},
			"validation_level": schema.StringAttribute{
				Optional: true,
				Computed: true,
				Default:  stringdefault.StaticString("strict"),
				Validators: []validator.String{
					stringvalidator.OneOf("off", "strict", "moderate"),
				},
			},
			"validation_action": schema.StringAttribute{
				Optional: true,
				Computed: true,
				Default:  stringdefault.StaticString("error"),
				Validators: []validator.String{
					stringvalidator.OneOf("error", "warn"),
				},
			},
		},
	}
}

func (r *Resource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	opts := &options.CreateCollectionOptions{}
	if v := plan.Validator.ValueString(); v != "" {
		var raw bson.Raw
		if err := bson.UnmarshalExtJSON([]byte(v), true, &raw); err != nil {
			resp.Diagnostics.AddError("invalid validator JSON", err.Error())
			return
		}
		opts.Validator = bson.M{"$jsonSchema": raw}
	}

	opts.ValidationLevel = plan.ValidationLevel.ValueStringPointer()
	opts.ValidationAction = plan.ValidationAction.ValueStringPointer()

	if err := r.client.Database(plan.Database.ValueString()).CreateCollection(ctx, plan.Name.ValueString(), opts); err != nil {
		resp.Diagnostics.AddError("create collection failed", err.Error())
		return
	}

	plan.ID = types.StringValue(fmt.Sprintf("%s/%s", plan.Database.ValueString(), plan.Name.ValueString()))
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.client.Database(state.Database.ValueString())
	res := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: state.Name.ValueString()}})
	if res.Err() != nil {
		// collection likely gone
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan ResourceModel
	var state ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Only validator-related updates via collMod
	db := r.client.Database(plan.Database.ValueString())
	cmd := bson.D{{Key: "collMod", Value: plan.Name.ValueString()}}

	if plan.Validator.ValueString() != state.Validator.ValueString() {
		if plan.Validator.ValueString() == "" {
			cmd = append(cmd, bson.E{Key: "validator", Value: bson.D{}})
		} else {
			var raw bson.Raw
			if err := bson.UnmarshalExtJSON([]byte(plan.Validator.ValueString()), true, &raw); err != nil {
				resp.Diagnostics.AddError("invalid validator JSON", err.Error())
				return
			}
			cmd = append(cmd, bson.E{Key: "validator", Value: raw})
		}
	}
	if plan.ValidationLevel.ValueString() != state.ValidationLevel.ValueString() {
		cmd = append(cmd, bson.E{Key: "validationLevel", Value: plan.ValidationLevel.ValueString()})
	}
	if plan.ValidationAction.ValueString() != state.ValidationAction.ValueString() {
		cmd = append(cmd, bson.E{Key: "validationAction", Value: plan.ValidationAction.ValueString()})
	}

	if len(cmd) > 1 { // we added something besides collMod name
		if err := db.RunCommand(ctx, cmd).Err(); err != nil {
			resp.Diagnostics.AddError("collMod failed", err.Error())
			return
		}
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.Database(state.Database.ValueString()).Collection(state.Name.ValueString()).Drop(ctx); err != nil {
		resp.Diagnostics.AddError("drop collection failed", err.Error())
	}
}
