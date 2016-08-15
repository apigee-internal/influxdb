package gossip

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

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
		s.Logger.Printf("owner.NodeID=%d, s.Client.ID=%d", owner.NodeID, s.Client.ID)
		if owner.NodeID == s.Client.ID {
			s.Logger.Printf("Calling actual CreateShard with ShardID=%d", shardID)
			err = s.Store.CreateShard(database, retentionPolicy, shardID, enabled)
			if err != nil {
				return err
			}
		} else {
			s.Logger.Printf("Calling remote CreateShard with ShardID=%d", shardID)
			err = s.CreateShardOnNode(nodes[owner.NodeID], database, retentionPolicy, shardID, enabled)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

// // CreateShard creates a shard with the given id and retention policy on a database.
// func (s *TSDBStore) CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error {
// 	return s.Store.CreateShard(database, retentionPolicy, shardID, enabled)
// }

// WriteToShard writes a list of points to a shard identified by its ID.
func (s *TSDBStore) WriteToShard(shardID uint64, points []models.Point) error {
	_, _, sgi := s.Client.ShardOwner(shardID)

	owners := sgi.ShardFor(shardID).Owners
	nodes, err := s.Client.AliveNodesMap()
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	defer wg.Wait()

	for _, owner := range owners {
		wg.Add(1)
		go func(owner meta.ShardOwner) error {
			defer wg.Done()
			s.Logger.Printf("owner.NodeID=%d, s.Client.ID=%d", owner.NodeID, s.Client.ID)
			if owner.NodeID == s.Client.ID {
				s.Logger.Printf("Callin	g actual WriteToShard with ShardID=%d", shardID)
				err = s.Store.WriteToShard(shardID, points)
				if err != nil {
					return err
				}
			}
			s.Logger.Printf("Calling remote WriteToShard with ShardID=%d", shardID)
			err = s.WriteToShardOnNode(nodes[owner.NodeID], shardID, points)
			if err != nil {
				return err
			}
			return nil
		}(owner)
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
	err = s.Store.WriteToShard(cmd.GetShardID(), points)
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
	url := "http://" + node.BindAddr + "/write"
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
	_, err = ExpBackoffRequest(*req)
	if err != nil {
		s.Logger.Printf("Failed to write shard to remote node with ID: %d", node.ID)
		return err
	}
	return nil
}

type remoteShardIteratorCreator struct {
	sh *RemoteIteratorCreator
}

func (ic *remoteShardIteratorCreator) Close() error { return nil }

func (ic *remoteShardIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return ic.sh.CreateIterator(opt)
}
func (ic *remoteShardIteratorCreator) FieldDimensions(sources influxql.Sources) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return ic.sh.FieldDimensions(sources)
}
func (ic *remoteShardIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return ic.sh.ExpandSources(sources)
}

//IteratorCreator foo
func (s *TSDBStore) IteratorCreator(shards []meta.ShardInfo, opt *influxql.SelectOptions) (influxql.IteratorCreator, error) {
	var shardIDs []uint64
	shardIDs = make([]uint64, 0)
	var remoteShardIDs []uint64
	remoteShardIDs = make([]uint64, 0)
	remoteShardMap := make(map[uint64][]uint64)
	var ok bool
	for _, sh := range shards {
		isNewRemote := 1
		remoteShardIDs = remoteShardIDs[:0]
		for _, owner := range sh.Owners {
			if owner.NodeID == s.Client.ID {
				shardIDs = append(shardIDs, sh.ID)
				isNewRemote = 0
				break
			} else if remoteShardIDs, ok = remoteShardMap[owner.NodeID]; ok {
				remoteShardIDs = append(remoteShardIDs, sh.ID)
				remoteShardMap[owner.NodeID] = remoteShardIDs
				isNewRemote = 0
				break
			}
		}
		if isNewRemote == 1 {
			remoteShardIDs = append(remoteShardIDs, sh.Owners[0].NodeID)
			remoteShardMap[sh.Owners[0].NodeID] = remoteShardIDs
		}
	}

	var localIC influxql.IteratorCreator
	var ics []influxql.IteratorCreator
	ics = make([]influxql.IteratorCreator, 0)
	var err error
	if err := func() error {
		for nodeID, remoteShardIDs := range remoteShardMap {
			ric := &remoteShardIteratorCreator{sh: &RemoteIteratorCreator{nodeID, remoteShardIDs}}
			ics = append(ics, ric)
		}
		localIC, err = s.Store.IteratorCreator(shardIDs, opt)
		return err
	}(); err != nil {
		influxql.IteratorCreators(ics).Close()
		return nil, err
	}
	// append the two slices
	ics = append(ics, localIC)
	return influxql.IteratorCreators(ics), nil
}

// ReadShardsToRemote foo
func (s *TSDBStore) ReadShardsToRemote(w http.ResponseWriter, r *http.Request) {
	cmd := ReadShardCommand{}
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

	// shards := s.Store.Shards(cmd.GetShardIDs())

	for _, shardID := range cmd.GetShardIDs() {
		var buf bytes.Buffer
		since, err := time.Parse(time.RFC3339Nano, strconv.FormatInt(cmd.GetStartTime(), 64))
		err = s.Store.BackupShard(shardID, since, &buf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("shard read = %s", buf.String())
	}
}
