package main

import (
	"github.com/TerrexTech/go-aggregate-user/authuser"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"

	mgo "github.com/mongodb/mongo-go-driver/mongo"

	"github.com/TerrexTech/go-commonutils/utils"
	cql "github.com/gocql/gocql"
	"github.com/pkg/errors"

	db "github.com/TerrexTech/go-aggregate-user/db"
	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/joho/godotenv"
)

// AggregateID is the aggregate-id (as stored in event-store)
// for the auth-user aggregate.
const AggregateID = 0

type EventStoreMeta struct {
	// AggregateVersion tracks the version to be used
	// by new events for that aggregate.
	AggregateVersion int64 `json:"aggregate_version"`
	// AggregateID corresponds to AggregateID in
	// event-store and ID in aggregate-projection.
	AggregateID int8 `json:"aggregate_id"`
	// Year bucket is the year in which the event was generated.
	// This is used as the partitioning key.
	YearBucket int16 `json:"year_bucket"`
}

func initCassandra() (*cs.Table, error) {
	hosts := os.Getenv("CASSANDRA_HOSTS")
	dataCenters := os.Getenv("CASSANDRA_DATA_CENTERS")
	username := os.Getenv("CASSANDRA_USERNAME")
	password := os.Getenv("CASSANDRA_PASSWORD")
	keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
	tableName := os.Getenv("CASSANDRA_TABLE")

	tableDef := &map[string]cs.TableColumn{
		"aggregateVersion": cs.TableColumn{
			Name:            "aggregate_version",
			DataType:        "int",
			PrimaryKeyIndex: "2",
			PrimaryKeyOrder: "DESC",
		},
		"aggregateId": cs.TableColumn{
			Name:            "aggregate_id",
			DataType:        "uuid",
			PrimaryKeyIndex: "1",
		},
		"yearBucket": cs.TableColumn{
			Name:            "year_bucket",
			DataType:        "smallint",
			PrimaryKeyIndex: "0",
		},
	}

	clusterHosts := *utils.ParseHosts(hosts)
	cluster := cql.NewCluster(clusterHosts...)
	cluster.ConnectTimeout = time.Millisecond * 3000
	cluster.Timeout = time.Millisecond * 3000
	cluster.ProtoVersion = 4

	if username != "" && password != "" {
		cluster.Authenticator = cql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	config := &db.CassandraConfig{
		Cluster:     cluster,
		DataCenters: utils.ParseHosts(dataCenters),
		Keyspace:    keyspaceName,
		Table:       tableName,
		TableDef:    tableDef,
	}
	return db.CassandraTable(config)
}

func initMongo() (*mongo.Collection, error) {
	hosts := os.Getenv("MONGO_HOSTS")
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	database := os.Getenv("MONGO_DATABASE")
	collection := os.Getenv("MONGO_COLLECTION")

	connStr := fmt.Sprintf("mongodb://%s:%s@%s", username, password, hosts)
	client, err := mgo.NewClient(connStr)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(1)*time.Second,
	)
	defer cancel()
	err = client.Connect(ctx)

	config := &db.MongoConfig{
		Client:     client,
		TimeoutMS:  1000,
		Database:   database,
		Collection: collection,
	}
	return db.MongoCollection(config)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	csTable, err := initCassandra()
	if err != nil {
		err = errors.Wrap(err, "Error Initializing Cassandra Table")
		log.Fatalln(err)
	}

	mgTable, err := initMongo()
	if err != nil {
		err = errors.Wrap(err, "Error Initializing Mongo Collection")
		log.Fatalln(err)
	}

	aggIDCol, err := csTable.Column("aggID")
	colValues := []cs.ColumnComparator{
		cs.ColumnComparator{
			Name:  aggIDCol,
			Value: AggregateID,
		}.Eq(),
	}

	eventMeta := []EventStoreMeta{}
	params := cs.SelectParams{
		ColumnValues: colValues,
		ResultsBind:  eventMeta,
	}
	_, err = csTable.Select(params)
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error Fetching Event-Meta for Aggregate: %d. "+
				"Will try again on another event.",
			AggregateID,
		)
		log.Println(err)
		return
	}

	// Update Version
	newVersion := eventMeta[0].AggregateVersion + 1
	eventMeta[0].AggregateVersion = newVersion
	err = <-csTable.AsyncInsert(eventMeta[0])
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error Updating Meta-Version for Aggregate: %d. "+
				"Will try again on another event.",
			AggregateID,
		)
		return
	}

	esMetaScanInterval := os.Getenv("EVENT_STORE_META_SCAN_INTERVAL_MILLISECONDS")


	// Fetch Current Projection-Version
	mgTable.Find(&authuser.Model{

	})

	scanInterval, err := time.ParseDuration(esMetaScanInterval + "ms")
	if err != nil {
		err = errors.Wrap(
			err,
			"Error Parsing EVENT_STORE_META_SCAN_INTERVAL to Integer",
		)
		log.Println(err)
		return
	}
	time.Sleep(scanInterval * time.Millisecond)

	// Fetch the events to be hydrated
	aggIDCol, err := csTable.Column("aggID")
	aggVersionCol, err := csTable.Column("aggregateVersion")
	yearBucketCol, err := csTable.Column("yearBucket")
	colValues := []cs.ColumnComparator{
		cs.ColumnComparator{
			Name:  aggIDCol,
			Value: AggregateID,
		}.Eq(),
		cs.ColumnComparator{
			Name: aggVersionCol,
			Value: newVersion,
		}.Gt(),
		cs.ColumnComparator{
			Name: aggVersionCol,
			Value: newVersion,
		}.Lt(),
	}

	aggEvents := &[]models.Event{}
	params := cs.SelectParams{
		ColumnValues: colValues,
		ResultsBind:  eventMeta,
	}
	_, err = csTable.Select(params)
	if err != nil {
		err = errors.Wrapf(
			err,
			"Error Fetching Event-Meta for Aggregate: %d. "+
				"Will try again on another event.",
			AggregateID,
		)
		log.Println(err)
		return
	}

}
