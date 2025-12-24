package index

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ExIndexSpecifications []*ExIndexSpecification

func (eis ExIndexSpecifications) Find(name string) *ExIndexSpecification {
	for _, i := range eis {
		if i != nil && i.Name == name {
			return i
		}
	}
	return nil
}

// ExIndexSpecification represents the specification of a MongoDB index.
// mongo.IndexSpecification has missing fields like PartialFilterExpression, so we define our own.
type ExIndexSpecification struct {
	Name                    string   `bson:"name"`
	Namespace               string   `bson:"ns"`
	KeysDocument            bson.Raw `bson:"key"`
	Version                 int32    `bson:"v"`
	ExpireAfterSeconds      *int32   `bson:"expireAfterSeconds"`
	Sparse                  *bool    `bson:"sparse"`
	Unique                  *bool    `bson:"unique"`
	Clustered               *bool    `bson:"clustered"`
	PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
}

type ExIndexView struct {
	mongo.IndexView
}

func (eiv ExIndexView) ListExSpecifications(ctx context.Context, opts ...*options.ListIndexesOptions) (ExIndexSpecifications, error) {
	cursor, err := eiv.IndexView.List(ctx, opts...)
	if err != nil {
		return nil, err
	}

	var results ExIndexSpecifications
	err = cursor.All(ctx, &results)
	if err != nil {
		return nil, err
	}

	return results, nil
}
