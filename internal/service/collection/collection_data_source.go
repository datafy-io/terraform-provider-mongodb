package collection

import (
	"context"
	"fmt"

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
	ID               types.String `tfsdk:"id"`
	Database         types.String `tfsdk:"database"`
	Name             types.String `tfsdk:"name"`
	Validator        types.String `tfsdk:"validator"`
	ValidationLevel  types.String `tfsdk:"validation_level"`
	ValidationAction types.String `tfsdk:"validation_action"`

	TimeSeries *TimeSeriesModel `tfsdk:"timeseries"`
}

func (d *DataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_collection"
}

func (d *DataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Retrieves a specific MongoDB collection.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
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
				Computed:    true,
				Description: "JSON string for validator (without the $jsonSchema prefix).",
			},
			"validation_level": schema.StringAttribute{
				Computed:    true,
				Description: "Validation level for the collection. Can be 'off', 'strict', or 'moderate'.",
			},
			"validation_action": schema.StringAttribute{
				Computed:    true,
				Description: "Action to take when validation fails. Can be 'error' or 'warn'.",
			},
		},
		Blocks: map[string]schema.Block{
			"timeseries": schema.SingleNestedBlock{
				Description: "MongoDB time-series collection options. If set, the collection will be created as a time-series collection.",
				Attributes: map[string]schema.Attribute{
					"time_field": schema.StringAttribute{
						Computed:    true,
						Description: "Name of the field that contains the date in each document.",
					},
					"meta_field": schema.StringAttribute{
						Computed:    true,
						Description: "Name of the field that contains metadata in each document.",
					},
					"granularity": schema.StringAttribute{
						Computed:    true,
						Description: "Time-series granularity. One of 'seconds', 'minutes', or 'hours'.",
					},
					"bucket_max_span_seconds": schema.Int64Attribute{
						Computed:    true,
						Description: "Maximum span (in seconds) for each bucket.",
					},
					"bucket_rounding_seconds": schema.Int64Attribute{
						Computed:    true,
						Description: "Rounding (in seconds) used to align bucket boundaries.",
					},
					"expire_after_seconds": schema.Int64Attribute{
						Computed:    true,
						Description: "TTL (in seconds) for time-series collections.",
					},
				},
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

	db := d.client.Database(plan.Database.ValueString())
	collections, err := db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: plan.Name.ValueString()}})
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
				plan.Validator = types.StringValue(string(jsonBytes))
			}
		} else {
			plan.Validator = types.StringNull()
		}

		if vl := collection.Options.Lookup("validationLevel"); vl.Type == bson.TypeString {
			plan.ValidationLevel = types.StringValue(vl.StringValue())
		} else {
			plan.ValidationLevel = types.StringValue("strict")
		}

		if va := collection.Options.Lookup("validationAction"); va.Type == bson.TypeString {
			plan.ValidationAction = types.StringValue(va.StringValue())
		} else {
			plan.ValidationAction = types.StringValue("error")
		}
	} else {
		plan.Validator = types.StringNull()
		plan.ValidationLevel = types.StringValue("strict")
		plan.ValidationAction = types.StringValue("error")
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

			plan.TimeSeries = &tsState
		} else {
			plan.TimeSeries = nil
		}
	} else {
		plan.TimeSeries = nil
	}

	plan.ID = types.StringValue(fmt.Sprintf("%s/%s", plan.Database.ValueString(), plan.Name.ValueString()))
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}
