package collection

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ resource.Resource = &Resource{}
var _ resource.ResourceWithConfigure = &Resource{}
var _ resource.ResourceWithImportState = &Resource{}

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
				Optional:    true,
				Computed:    true,
				Description: "Validation level for the collection. Can be 'off', 'strict', or 'moderate'. (Default: 'strict')",
				Default:     stringdefault.StaticString("strict"),
				Validators: []validator.String{
					stringvalidator.OneOf("off", "strict", "moderate"),
				},
			},
			"validation_action": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Action to take when validation fails. Can be 'error' or 'warn'. (Default: 'error')",
				Default:     stringdefault.StaticString("error"),
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
	collections, err := db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: state.Name.ValueString()}})
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading collection",
			fmt.Sprintf("Failed to list collections: %s", err),
		)
		return
	}
	if collections == nil || len(collections) != 1 {
		resp.Diagnostics.AddError(
			"Collection not found", fmt.Sprintf("%d", len(collections)),
		)
		return
	}

	collection := collections[0]
	if collection.Options != nil {
		if v := collection.Options.Lookup("validator"); v.Type == bson.TypeEmbeddedDocument {
			doc := v.Document()
			jsonBytes, err := bson.MarshalExtJSON(doc, true, true)
			if err != nil {
				resp.Diagnostics.AddWarning("Failed to encode validator", fmt.Sprintf("validator extjson encode error: %v", err))
			} else {
				state.Validator = types.StringValue(string(jsonBytes))
			}
		} else {
			state.Validator = types.StringNull()
		}

		if vl := collection.Options.Lookup("validationLevel"); vl.Type == bson.TypeString {
			state.ValidationLevel = types.StringValue(vl.StringValue())
		} else {
			state.ValidationLevel = types.StringNull()
		}

		if va := collection.Options.Lookup("validationAction"); va.Type == bson.TypeString {
			state.ValidationAction = types.StringValue(va.StringValue())
		} else {
			state.ValidationAction = types.StringNull()
		}
	} else {
		state.Validator = types.StringNull()
		state.ValidationLevel = types.StringNull()
		state.ValidationAction = types.StringNull()
	}

	state.ID = types.StringValue(fmt.Sprintf("%s/%s", state.Database.ValueString(), state.Name.ValueString()))
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

func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := strings.TrimSpace(req.ID)
	if id == "" {
		resp.Diagnostics.AddError(
			"Empty import ID",
			"Expected format: 'database/collection'",
		)
		return
	}

	parts := strings.SplitN(id, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		resp.Diagnostics.AddError(
			"Invalid import ID",
			fmt.Sprintf("Expected 'database/collection', got %s", id),
		)
		return
	}
	db, coll := parts[0], parts[1]

	var state ResourceModel
	state.ID = types.StringValue(id)
	state.Name = types.StringValue(coll)
	state.Database = types.StringValue(db)

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
