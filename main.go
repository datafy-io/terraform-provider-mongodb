package main

import (
	"context"
	"flag"
	"log"

	"github.com/datafy-io/terraform-provider-mongodb/internal/provider"
	"github.com/datafy-io/terraform-provider-mongodb/version"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	opts := providerserver.ServeOpts{
		Address: "registry.terraform.io/datafy-io/mongodb",
		Debug:   debug,
	}

	err := providerserver.Serve(context.Background(), provider.New(version.ProviderVersion), opts)

	if err != nil {
		log.Fatal(err.Error())
	}
}
