package database

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const tfPlaceholderColl = "__tf_placeholder"

// Ensure implementation satisfies interfaces.
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
	ID              types.String `tfsdk:"id"`
	Name            types.String `tfsdk:"name"`
	KeepPlaceholder types.Bool   `tfsdk:"keep_placeholder"`
}

func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
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
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Database name.",
			},
			"keep_placeholder": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(true),
				Description: "Keep a tiny placeholder collection so the DB persists. (Default: true)",
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

	dbs, err := r.client.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: plan.Name.ValueString()}})
	if err != nil {
		resp.Diagnostics.AddError("List databases failed", err.Error())
		return
	}
	if len(dbs) > 0 {
		resp.Diagnostics.AddError(
			"Database already exists",
			fmt.Sprintf("A database named %s already exists.", plan.Name.ValueString()),
		)
		return
	}

	db := r.client.Database(plan.Name.ValueString())

	if plan.KeepPlaceholder.ValueBool() {
		// create placeholder collection (ignore if exists)
		_ = db.RunCommand(ctx, bson.D{{Key: "create", Value: tfPlaceholderColl}}).Err()
	}

	plan.ID = types.StringValue(plan.Name.ValueString())
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.client.Database(state.Name.ValueString())
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		resp.Diagnostics.AddError("list collections failed", err.Error())
		return
	}
	if len(names) == 0 {
		// DB likely gone
		resp.State.RemoveResource(ctx)
		return
	}

	state.ID = types.StringValue(state.Name.ValueString())
	state.KeepPlaceholder = types.BoolValue(slices.Contains(names, tfPlaceholderColl))
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// No updatable attributes (name is ForceNew semantically). We just refresh state.
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	db := r.client.Database(plan.Name.ValueString())
	if plan.KeepPlaceholder.ValueBool() {
		// create placeholder collection (ignore if exists)
		_ = db.RunCommand(ctx, bson.D{{Key: "create", Value: tfPlaceholderColl}}).Err()
	} else {
		_ = db.RunCommand(ctx, bson.D{{Key: "drop", Value: tfPlaceholderColl}}).Err()
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := r.client.Database(state.Name.ValueString()).Drop(ctx); err != nil {
		resp.Diagnostics.AddError("failed to drop database", err.Error())
	}
}

func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := strings.TrimSpace(req.ID)
	if id == "" {
		resp.Diagnostics.AddError("Empty import ID", "Expected database name")
		return
	}

	var state ResourceModel
	state.ID = types.StringValue(id)
	state.Name = types.StringValue(id)

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
