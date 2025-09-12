package collection

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
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

type TimeSeriesModel struct {
	TimeField             types.String `tfsdk:"time_field"`
	MetaField             types.String `tfsdk:"meta_field"`
	Granularity           types.String `tfsdk:"granularity"`
	BucketMaxSpanSeconds  types.Int64  `tfsdk:"bucket_max_span_seconds"`
	BucketRoundingSeconds types.Int64  `tfsdk:"bucket_rounding_seconds"`
	ExpireAfterSeconds    types.Int64  `tfsdk:"expire_after_seconds"`
}

type ResourceModel struct {
	ID               types.String `tfsdk:"id"`
	Database         types.String `tfsdk:"database"`
	Name             types.String `tfsdk:"name"`
	Validator        types.String `tfsdk:"validator"`
	ValidationLevel  types.String `tfsdk:"validation_level"`
	ValidationAction types.String `tfsdk:"validation_action"`

	TimeSeries *TimeSeriesModel `tfsdk:"timeseries"`
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
		Description: "Manages a MongoDB collection.",
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
				Default:     stringdefault.StaticString("strict"),
				Description: "Validation level for the collection. Can be 'off', 'strict', or 'moderate'.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
				Validators: []validator.String{
					stringvalidator.OneOf("off", "strict", "moderate"),
				},
			},
			"validation_action": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString("error"),
				Description: "Action to take when validation fails. Can be 'error' or 'warn'.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
				Validators: []validator.String{
					stringvalidator.OneOf("error", "warn"),
				},
			},
		},
		Blocks: map[string]schema.Block{
			"timeseries": schema.SingleNestedBlock{
				Description: "MongoDB time-series collection options. If set, the collection will be created as a time-series collection.",
				Attributes: map[string]schema.Attribute{
					"time_field": schema.StringAttribute{
						Optional:    true,
						Description: "Name of the field that contains the date in each document.",
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.UseStateForUnknown(),
						},
					},
					"meta_field": schema.StringAttribute{
						Optional:    true,
						Description: "Name of the field that contains metadata in each document.",
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.UseStateForUnknown(),
						},
					},
					"granularity": schema.StringAttribute{
						Optional:    true,
						Description: "Time-series granularity. One of 'seconds', 'minutes', or 'hours'.",
						Validators: []validator.String{
							stringvalidator.OneOf("seconds", "minutes", "hours"),
						},
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.UseStateForUnknown(),
						},
					},
					"bucket_max_span_seconds": schema.Int64Attribute{
						Optional:    true,
						Description: "Maximum span (in seconds) for each bucket.",
						PlanModifiers: []planmodifier.Int64{
							int64planmodifier.UseStateForUnknown(),
						},
					},
					"bucket_rounding_seconds": schema.Int64Attribute{
						Optional: true,

						Description: "Rounding (in seconds) used to align bucket boundaries.",
						PlanModifiers: []planmodifier.Int64{
							int64planmodifier.UseStateForUnknown(),
						},
					},
					"expire_after_seconds": schema.Int64Attribute{
						Optional:    true,
						Description: "TTL (in seconds) for time-series collections.",
						PlanModifiers: []planmodifier.Int64{
							int64planmodifier.UseStateForUnknown(),
						},
					},
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
	if !plan.ValidationLevel.IsNull() && !plan.ValidationLevel.IsUnknown() {
		opts.ValidationLevel = plan.ValidationLevel.ValueStringPointer()
	}
	if !plan.ValidationAction.IsNull() && !plan.ValidationAction.IsUnknown() {
		opts.ValidationAction = plan.ValidationAction.ValueStringPointer()
	}

	if plan.TimeSeries != nil {
		ts := options.TimeSeries()
		ts.SetTimeField(plan.TimeSeries.TimeField.ValueString())

		if v := plan.TimeSeries.MetaField.ValueString(); v != "" {
			ts = ts.SetMetaField(v)
		}
		if v := plan.TimeSeries.Granularity.ValueString(); v != "" {
			ts = ts.SetGranularity(v)
		}
		if !plan.TimeSeries.BucketMaxSpanSeconds.IsNull() && !plan.TimeSeries.BucketMaxSpanSeconds.IsUnknown() {
			ts = ts.SetBucketMaxSpan(time.Duration(plan.TimeSeries.BucketMaxSpanSeconds.ValueInt64()) * time.Second)
		}
		if !plan.TimeSeries.BucketRoundingSeconds.IsNull() && !plan.TimeSeries.BucketRoundingSeconds.IsUnknown() {
			ts = ts.SetBucketRounding(time.Duration(plan.TimeSeries.BucketRoundingSeconds.ValueInt64()) * time.Second)
		}
		if !plan.TimeSeries.ExpireAfterSeconds.IsNull() && !plan.TimeSeries.ExpireAfterSeconds.IsUnknown() {
			opts = opts.SetExpireAfterSeconds(plan.TimeSeries.ExpireAfterSeconds.ValueInt64())
		}

		opts = opts.SetTimeSeriesOptions(ts)
	}

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
		}

		if va := collection.Options.Lookup("validationAction"); va.Type == bson.TypeString {
			state.ValidationAction = types.StringValue(va.StringValue())
		}
	} else {
		state.Validator = types.StringNull()
		state.ValidationLevel = types.StringNull()
		state.ValidationAction = types.StringNull()
	}

	if collection.Options != nil {
		if tsVal := collection.Options.Lookup("timeseries"); tsVal.Type == bson.TypeEmbeddedDocument {
			tsDoc := tsVal.Document()
			var tsState TimeSeriesModel

			if f := tsDoc.Lookup("timeField"); f.Type == bson.TypeString {
				tsState.TimeField = types.StringValue(f.StringValue())
			}
			if f := tsDoc.Lookup("metaField"); f.Type == bson.TypeString {
				tsState.MetaField = types.StringValue(f.StringValue())
			} else {
				tsState.MetaField = types.StringNull()
			}
			if f := tsDoc.Lookup("granularity"); f.Type == bson.TypeString {
				tsState.Granularity = types.StringValue(f.StringValue())
			} else {
				tsState.Granularity = types.StringNull()
			}
			if value, ok := tsDoc.Lookup("bucketMaxSpanSeconds").AsInt64OK(); ok {
				tsState.BucketMaxSpanSeconds = types.Int64Value(value)
			} else {
				tsState.BucketMaxSpanSeconds = types.Int64Null()
			}
			if value, ok := tsDoc.Lookup("bucketRoundingSeconds").AsInt64OK(); ok {
				tsState.BucketRoundingSeconds = types.Int64Value(value)
			} else {
				tsState.BucketRoundingSeconds = types.Int64Null()
			}

			if value, ok := collection.Options.Lookup("expireAfterSeconds").AsInt64OK(); ok {
				tsState.ExpireAfterSeconds = types.Int64Value(value)
			} else {
				tsState.ExpireAfterSeconds = types.Int64Null()
			}

			state.TimeSeries = &tsState
		} else {
			state.TimeSeries = nil
		}
	} else {
		state.TimeSeries = nil
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
