package providerdata

import (
	"fmt"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"go.mongodb.org/mongo-driver/mongo"
)

// Data is what the provider hands its resources and data sources: a MongoDB client when `uri` is set,
// and an App Services Admin API client when the Atlas API key is set.
type Data struct {
	Mongo       *mongo.Client
	AppServices *appservices.Client
}

// MongoClient returns the MongoDB client, for databases, collections and indexes.
func MongoClient(providerData any) (*mongo.Client, diag.Diagnostics) {
	var diags diag.Diagnostics

	data, ok := providerData.(*Data)
	if !ok {
		diags.AddError(
			"Unexpected Configure Type",
			fmt.Sprintf("Expected *providerdata.Data, got: %T. Please report this issue to the provider developers.", providerData),
		)
		return nil, diags
	}
	if data.Mongo == nil {
		diags.AddError("Missing MongoDB URI", "Set uri in the provider block to manage databases, collections and indexes.")
	}

	return data.Mongo, diags
}

// AppServicesClient returns the App Services Admin API client, for App Services apps and functions.
func AppServicesClient(providerData any) (*appservices.Client, diag.Diagnostics) {
	var diags diag.Diagnostics

	data, ok := providerData.(*Data)
	if !ok {
		diags.AddError(
			"Unexpected Configure Type",
			fmt.Sprintf("Expected *providerdata.Data, got: %T. Please report this issue to the provider developers.", providerData),
		)
		return nil, diags
	}
	if data.AppServices == nil {
		diags.AddError("Missing Atlas API key", "Add an atlas block with public_key and private_key to the provider block to manage App Services apps and functions.")
	}

	return data.AppServices, diags
}
