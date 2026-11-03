package app_services_function

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/datafy-io/terraform-provider-mongodb/internal/providerdata"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure implementation satisfies interfaces.
var _ resource.Resource = &Resource{}
var _ resource.ResourceWithConfigure = &Resource{}
var _ resource.ResourceWithImportState = &Resource{}

// importIdSeparator matches mongodbatlas_event_trigger's import ID, so a function and its trigger import the same way.
const importIdSeparator = "--"

func NewResource() resource.Resource {
	return &Resource{}
}

type Resource struct {
	client *appservices.Client
}

type ResourceModel struct {
	ID             types.String `tfsdk:"id"`
	ProjectId      types.String `tfsdk:"project_id"`
	AppId          types.String `tfsdk:"app_id"`
	Name           types.String `tfsdk:"name"`
	Source         types.String `tfsdk:"source"`
	Private        types.Bool   `tfsdk:"private"`
	RunAsSystem    types.Bool   `tfsdk:"run_as_system"`
	PreventDestroy types.Bool   `tfsdk:"prevent_destroy"`
}

func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_app_services_function"
}

func (r *Resource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages an Atlas App Services [Function](https://www.mongodb.com/docs/atlas/app-services/functions/), which " +
			"`mongodbatlas_event_trigger` can run. Needs `atlas_public_key` and `atlas_private_key` in the provider block. " +
			"An update replaces the whole function, so settings this resource does not manage " +
			"(`can_evaluate`, `run_as_user_id`, `run_as_user_id_script_source`) are cleared.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:    true,
				Description: "The function's ObjectID. Pass it to `mongodbatlas_event_trigger.function_id`.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"project_id": schema.StringAttribute{
				Required:    true,
				Description: "The Atlas project ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"app_id": schema.StringAttribute{
				Required:    true,
				Description: "The App Services app's ObjectID (`id` of the `mongodb_app_services_app` data source), not its client App ID.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Function name, unique within the app.",
			},
			"source": schema.StringAttribute{
				Required:    true,
				Description: "The function's JavaScript source, e.g. `file(\"functions/job.js\")`.",
			},
			"private": schema.BoolAttribute{
				Required:    true,
				Description: "Whether the function is hidden from client applications. Triggers can still call a private function.",
			},
			"run_as_system": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "Whether the function runs as the system user, bypassing rules. (Default: false)",
			},
			"prevent_destroy": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				Description: "If true, prevents the function from being destroyed. (Default: false)",
			},
		},
	}
}

func (r *Resource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, diags := providerdata.AppServicesClient(req.ProviderData)
	resp.Diagnostics.Append(diags...)
	r.client = client
}

func (r *Resource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	cfr, err := r.client.CreateFunction(ctx, &appservices.CreateFunctionRequest{
		ProjectId: plan.ProjectId.ValueString(),
		AppId:     plan.AppId.ValueString(),
		Function:  toFunction(plan),
	})
	if err != nil {
		resp.Diagnostics.AddError("Create function failed", fmt.Sprintf("Could not create function %s: %s", plan.Name.ValueString(), err))
		return
	}

	plan.ID = types.StringValue(cfr.Function.FunctionId)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	gfr, err := r.client.GetFunction(ctx, &appservices.GetFunctionRequest{
		ProjectId:  state.ProjectId.ValueString(),
		AppId:      state.AppId.ValueString(),
		FunctionId: state.ID.ValueString(),
	})
	if errors.Is(err, appservices.ErrNotFound) {
		// Deleted outside Terraform: plan to create it again.
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Read function failed", fmt.Sprintf("Could not read function %s: %s", state.ID.ValueString(), err))
		return
	}

	state.ID = types.StringValue(gfr.Function.FunctionId)
	state.Name = types.StringValue(gfr.Function.Name)
	state.Source = types.StringValue(gfr.Function.Source)
	state.Private = types.BoolValue(gfr.Function.Private)
	state.RunAsSystem = types.BoolValue(gfr.Function.RunAsSystem)
	if state.PreventDestroy.IsNull() {
		// Just imported.
		state.PreventDestroy = types.BoolValue(false)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan ResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := r.client.UpdateFunction(ctx, &appservices.UpdateFunctionRequest{
		ProjectId: plan.ProjectId.ValueString(),
		AppId:     plan.AppId.ValueString(),
		Function:  toFunction(plan),
	})
	if err != nil {
		resp.Diagnostics.AddError("Update function failed", fmt.Sprintf("Could not update function %s: %s", plan.ID.ValueString(), err))
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

	if state.PreventDestroy.ValueBool() {
		resp.Diagnostics.AddError("Prevented Function Deletion", "The function is marked as prevent_destroy, so it will not be deleted.")
		return
	}

	_, err := r.client.DeleteFunction(ctx, &appservices.DeleteFunctionRequest{
		ProjectId:  state.ProjectId.ValueString(),
		AppId:      state.AppId.ValueString(),
		FunctionId: state.ID.ValueString(),
	})
	if err != nil && !errors.Is(err, appservices.ErrNotFound) {
		resp.Diagnostics.AddError("Delete function failed", fmt.Sprintf("Could not delete function %s: %s", state.ID.ValueString(), err))
	}
}

// ImportState takes `<project_id>--<app_id>--<function>`, where <function> is the function's ID or its name.
// Read then fills in the rest.
func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.SplitN(strings.TrimSpace(req.ID), importIdSeparator, 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		resp.Diagnostics.AddError(
			"Invalid import ID",
			fmt.Sprintf("Expected <project_id>%[1]s<app_id>%[1]s<function ID or name>, got %q.", importIdSeparator, req.ID),
		)
		return
	}
	projectId, appId, function := parts[0], parts[1], parts[2]

	lfr, err := r.client.ListFunctions(ctx, &appservices.ListFunctionsRequest{
		ProjectId: projectId,
		AppId:     appId,
	})
	if err != nil {
		resp.Diagnostics.AddError("Import function failed", fmt.Sprintf("Could not list the functions of app %s: %s", appId, err))
		return
	}

	functionId := ""
	for _, f := range lfr.Functions {
		if f.FunctionId == function || f.Name == function {
			functionId = f.FunctionId
			break
		}
	}
	if functionId == "" {
		resp.Diagnostics.AddError("Import function failed", fmt.Sprintf("App %s has no function with ID or name %q.", appId, function))
		return
	}

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), functionId)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("project_id"), projectId)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("app_id"), appId)...)
}

func toFunction(m ResourceModel) appservices.Function {
	return appservices.Function{
		FunctionId:  m.ID.ValueString(),
		Name:        m.Name.ValueString(),
		Source:      m.Source.ValueString(),
		Private:     m.Private.ValueBool(),
		RunAsSystem: m.RunAsSystem.ValueBool(),
	}
}
