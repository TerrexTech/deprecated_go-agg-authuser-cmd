package db

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	cql "github.com/gocql/gocql"
)

// CassandraConfig defines the Configuration for creating the
// specified Cassandra Keyspace and Table.
type CassandraConfig struct {
	Cluster     *cql.ClusterConfig
	DataCenters *[]string
	Keyspace    string
	Table       string
	TableDef    *map[string]cs.TableColumn
}

// CassandraTable creates the required Cassandra Keyspace and Table
// if they don't exist. If the specified Keyspace or Table or both
// already exist, this method will just create CassandraUtils table
// without altering the existing Keyspace and Table.
func CassandraTable(config *CassandraConfig) (*cs.Table, error) {
	session, err := cs.GetSession(config.Cluster)
	if err != nil {
		err = errors.Wrap(err, "Error Getting Cassandra Session")
		return nil, err
	}

	datacenterMap := map[string]int{}
	for _, dcStr := range *config.DataCenters {
		dc := strings.Split(dcStr, ":")
		centerID, err := strconv.Atoi(dc[1])
		if err != nil {
			return nil, errors.Wrap(
				err,
				"Cassandra Keyspace Create Error (CASSANDRA_DATA_CENTERS format mismatch)"+
					"CASSANDRA_DATA_CENTERS must be of format \"<ID>:<replication_factor>\"",
			)
		}
		datacenterMap[dc[0]] = centerID
	}

	keyspaceConfig := cs.KeyspaceConfig{
		Name:                    config.Keyspace,
		ReplicationStrategy:     "NetworkTopologyStrategy",
		ReplicationStrategyArgs: datacenterMap,
	}
	keyspace, err := cs.NewKeyspace(session, keyspaceConfig)
	if err != nil {
		err = errors.Wrap(err, "Error Creating Cassandra Keyspace")
		return nil, err
	}

	tc := &cs.TableConfig{
		Keyspace: keyspace,
		Name:     config.Table,
	}

	table, err := cs.NewTable(session, tc, config.TableDef)
	if err != nil {
		err = errors.Wrap(err, "Error Creating Cassandra Table")
		return nil, err
	}
	return table, nil
}
