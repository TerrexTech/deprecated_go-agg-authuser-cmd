package db

import (
	"github.com/TerrexTech/go-aggregate-user/authuser"
	"github.com/TerrexTech/go-mongoutils/mongo"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type MongoConfig struct {
	Client     *mgo.Client
	TimeoutMS  int16
	Database   string
	Collection string
}

func MongoCollection(config *MongoConfig) (*mongo.Collection, error) {
	conn := &mongo.ConnectionConfig{
		Client:  config.Client,
		Timeout: 5000,
	}

	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "name",
					IsDescOrder: true,
				},
				mongo.IndexColumnConfig{
					Name:        "email",
					IsDescOrder: true,
				},
			},
			IsUnique: true,
			Name:     "identity_index",
		},
	}

	c := &mongo.Collection{
		Connection:   conn,
		Name:         config.Collection,
		Database:     config.Database,
		SchemaStruct: &authuser.Model{},
		Indexes:      indexConfigs,
	}

	coll, err := mongo.EnsureCollection(c)
	if err != nil {
		err = errors.Wrap(err, "Error Creating Mongo Collection")
		return nil, err
	}
	return coll, nil
}
