package gossip

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/clusterflux"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

// TSDBStore manages shards and indexes for databases.
type TSDBStore struct {
	*tsdb.Store
	Logger *log.Logger
	Client *cflux.Client
}

// NewStore returns a new store with the given path and a default configuration.
// The returned store must be initialized by calling Open before using it.
func NewStore(path string, client *cflux.Client) *TSDBStore {
	return &TSDBStore{
		Store:  tsdb.NewStore(path),
		Client: client,
		Logger: log.New(os.Stderr, "[clusterfluxStore] ", log.LstdFlags),
	}
}

// Open initializes the store, creating all necessary directories, loading all
// shards and indexes and initializing periodic maintenance of all shards.
func (s *TSDBStore) Open() error {
	err := s.Store.Open()
	if err != nil {
		return err
	}
	go s.StartRouter()
	return nil
}

// CreateShard creates a shard with the given id and retention policy on a database.
func (s *TSDBStore) CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error {
	db, policy, sgi := s.Client.ShardOwner(shardID)
	if db != database || policy != retentionPolicy {
		s.Logger.Println("Error getting the shard with specified ID")
		return errors.New("Error getting the shard with specified ID")
	}

	owners := sgi.ShardFor(shardID).Owners
	nodes, err := s.Client.AliveNodesMap()
	if err != nil {
		return err
	}

	for _, owner := range owners {
		if owner.NodeID == s.Client.ID {
			s.Store.CreateShard(database, retentionPolicy, shardID, enabled)
		} else {
			err = s.CreateShardOnNode(nodes[owner.NodeID], database, retentionPolicy, shardID, enabled)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

// WriteToShard writes a list of points to a shard identified by its ID.
func (s *TSDBStore) WriteToShard(shardID uint64, points []models.Point) error {
	_, _, sgi := s.Client.ShardOwner(shardID)

	owners := sgi.ShardFor(shardID).Owners
	nodes, err := s.Client.AliveNodesMap()
	if err != nil {
		return err
	}

	for _, owner := range owners {
		if owner.NodeID == s.Client.ID {
			s.Store.WriteToShard(shardID, points)
		} else {
			err = s.WriteToShardOnNode(nodes[owner.NodeID], shardID, points)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

// CreateShardLocal foo
func (s *TSDBStore) CreateShardLocal(w http.ResponseWriter, r *http.Request) {
	cmd := CreateShardCommmand{}
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Printf("Failed while reading received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = proto.Unmarshal(reqBody, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Store.CreateShard(cmd.GetDatabase(), cmd.GetRetentionPolicy(), cmd.GetShardID(), cmd.GetEnabled())
	if err != nil {
		output, _ := json.Marshal(err)
		buffer := bytes.NewBuffer(output)
		w.Write(buffer.Bytes())
	}
}

// WriteToShardLocal foo
func (s *TSDBStore) WriteToShardLocal(w http.ResponseWriter, r *http.Request) {
	points := make([]models.Point, 0, 10)

	cmd := WriteShardCommand{}
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Printf("Failed while reading received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = proto.Unmarshal(reqBody, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var pt models.Point
	for _, point := range cmd.Points {
		pt, err = models.NewPointFromBytes(point)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		points = append(points, pt)

	}
	err = s.WriteToShard(cmd.GetShardID(), points)
	if err != nil {
		output, _ := json.Marshal(err)
		buffer := bytes.NewBuffer(output)
		w.Write(buffer.Bytes())
	}
}

// CreateShardOnNode foo
func (s *TSDBStore) CreateShardOnNode(node cflux.NodesList, database string, retentionPolicy string, shardID uint64, enabled bool) error {
	url := "http://" + node.BindAddr + "/create"
	cmd := &CreateShardCommmand{Database: proto.String(database),
		RetentionPolicy: proto.String(retentionPolicy),
		ShardID:         proto.Uint64(shardID),
		Enabled:         proto.Bool(enabled)}

	data, err := proto.Marshal(cmd)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	_, err = s.Client.ExpBackoffRequest(*req)
	if err != nil {
		s.Logger.Printf("Failed to create shard on remote node with ID: %d", node.ID)
		return err
	}
	return nil
}

// WriteToShardOnNode foo
func (s *TSDBStore) WriteToShardOnNode(node cflux.NodesList, shardID uint64, points []models.Point) error {
	url := "http://" + "node.BindAddr" + "/write"
	pnts := make([][]byte, 0, len(points))
	for _, point := range points {
		data, err := point.MarshalBinary()
		if err != nil {
			return err
		}
		pnts = append(pnts, data)
	}

	cmd := &WriteShardCommand{
		ShardID: proto.Uint64(shardID),
		Points:  pnts}

	data, err := proto.Marshal(cmd)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	_, err = s.Client.ExpBackoffRequest(*req)
	if err != nil {
		s.Logger.Printf("Failed to write shard to remote node with ID: %d", node.ID)
		return err
	}
	return nil
}

//IteratorCreator foo
func (s *TSDBStore) IteratorCreator(shards []meta.ShardInfo, opt *influxql.SelectOptions) (influxql.IteratorCreator, error) {
	shardIDs := make([]uint64, len(shards))
	for i, sh := range shards {
		shardIDs[i] = sh.ID
	}
	return s.Store.IteratorCreator(shardIDs, opt)
}
