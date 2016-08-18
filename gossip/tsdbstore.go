package gossip

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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
			err = s.Store.CreateShard(database, retentionPolicy, shardID, enabled)
			if err != nil {
				s.Logger.Printf("Error creating shard on local node. Error: %s", err.Error())
				return err
			}
		} else {
			err = s.CreateShardOnNode(nodes[owner.NodeID], database, retentionPolicy, shardID, enabled)
			if err != nil {
				s.Logger.Printf("Error creating shard on remote node. Error: %s", err.Error())
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
	// The commented out code is used to parallelize the write.
	// We would need to use channels for communication of err when we implement this.
	// The error needs to be sent to the caller in order to call create shard in case
	// shard doesn't exist.

	// wg := sync.WaitGroup{}
	// defer wg.Wait()

	for _, owner := range owners {
		// wg.Add(1)
		// go func(owner meta.ShardOwner) error {
		// defer wg.Done()
		if owner.NodeID == s.Client.ID {
			err = s.Store.WriteToShard(shardID, points)
		} else {
			err = s.WriteToShardOnNode(nodes[owner.NodeID], shardID, points)
		}
		if err != nil {
			s.Logger.Printf("Failed to write: %s", err.Error())
			return err
		}
		// }(owner)
	}
	return err
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
		log.Printf("Failed while unmarshaling received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Store.CreateShard(cmd.Database, cmd.RetentionPolicy, cmd.ShardID, cmd.Enabled)
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
		log.Printf("Failed while unmarshaling received http body: %s", err.Error())
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
	err = s.Store.WriteToShard(cmd.ShardID, points)
	if err != nil {
		output, _ := json.Marshal(err)
		buffer := bytes.NewBuffer(output)
		w.Write(buffer.Bytes())
	}
}

// CreateShardOnNode foo
func (s *TSDBStore) CreateShardOnNode(node cflux.NodesList, database string, retentionPolicy string, shardID uint64, enabled bool) error {
	cmd := &CreateShardCommmand{Database: database,
		RetentionPolicy: retentionPolicy,
		ShardID:         shardID,
		Enabled:         enabled}

	data, err := proto.Marshal(cmd)

	f := func() (*http.Request, error) {
		url := "http://" + node.BindAddress + "/create"
		return http.NewRequest("POST", url, bytes.NewBuffer(data))
	}

	_, err = s.Client.ExpBackoffRequest(f)
	if err != nil {
		s.Logger.Printf("Failed to create shard on remote node with ID: %d", node.ID)
		return err
	}
	return nil
}

// WriteToShardOnNode foo
func (s *TSDBStore) WriteToShardOnNode(node cflux.NodesList, shardID uint64, points []models.Point) error {
	pnts := make([][]byte, 0, len(points))
	for _, point := range points {
		data, err := point.MarshalBinary()
		if err != nil {
			return err
		}
		pnts = append(pnts, data)
	}

	cmd := &WriteShardCommand{
		ShardID: shardID,
		Points:  pnts}

	data, err := proto.Marshal(cmd)

	f := func() (*http.Request, error) {
		url := "http://" + node.BindAddress + "/write"
		return http.NewRequest("POST", url, bytes.NewBuffer(data))
	}

	_, err = s.Client.ExpBackoffRequest(f)
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
	var localShardIDs []uint64
	var ics []influxql.IteratorCreator
	for _, sh := range shards {
		isRemote := 1
		for _, owner := range sh.Owners {
			if owner.NodeID == s.Client.ID {
				localShardIDs = append(localShardIDs, sh.ID)
				isRemote = 0
				break
			}
		}
		if isRemote == 1 {
			ric := &remoteShardIteratorCreator{sh: &RemoteIteratorCreator{Store: s, ShardID: sh.ID, NodeID: sh.Owners[0].NodeID}}
			ics = append(ics, ric)
		}
	}

	if err := func() error {
		for _, id := range localShardIDs {
			lic := s.Store.ShardIteratorCreator(id)
			if lic == nil {
				continue
			}
			ics = append(ics, lic)
		}
		return nil
	}(); err != nil {
		influxql.IteratorCreators(ics).Close()
		return nil, err
	}
	return influxql.IteratorCreators(ics), nil
}

// ReadShardToRemote foo
func (s *TSDBStore) ReadShardToRemote(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
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
		log.Printf("Failed while unmarshaling received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	optBinary := cmd.IteratorOptions
	opt := &influxql.IteratorOptions{}
	err = opt.UnmarshalBinary(optBinary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	shard := s.Store.Shard(cmd.ShardID)
	iter, err := shard.CreateIterator(*opt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	enc := influxql.NewIteratorEncoder(&buf)
	err = enc.EncodeIterator(iter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	shardID := cmd.ShardID

	var iterType ReadShardCommandResponse_IteratorType
	switch iter.(type) {
	case influxql.FloatIterator:
		iterType = ReadShardCommandResponse_FLOAT
	case influxql.IntegerIterator:
		iterType = ReadShardCommandResponse_INTEGER
	case influxql.StringIterator:
		iterType = ReadShardCommandResponse_STRING
	case influxql.BooleanIterator:
		iterType = ReadShardCommandResponse_BOOLEAN
	default:
		http.Error(w, fmt.Sprintf("Unsupported iterator for encoder: %T", iter), http.StatusBadRequest)
		return
	}

	resp := &ReadShardCommandResponse{ShardID: shardID, Type: iterType, Points: buf.Bytes()}
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// FieldDimensions foo
func (s *TSDBStore) FieldDimensions(w http.ResponseWriter, r *http.Request) {
	cmd := &FieldDimensionsCommand{}
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Printf("Failed while reading received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = proto.Unmarshal(reqBody, cmd)
	if err != nil {
		log.Printf("Failed while unmarshaling received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	shard := s.Store.Shard(cmd.ShardID)

	var sources influxql.Sources
	err = sources.UnmarshalBinary(cmd.Sources)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := &FieldDimensionsCommandResponse{Fields: make(map[string]uint32)}
	fields, dimensions, err := shard.FieldDimensions(sources)
	if err != nil {
		resp.Error = err.Error()
	} else {
		for key, value := range fields {
			resp.Fields[key] = uint32(value)
		}
		for key := range dimensions {
			resp.Dimensions = append(resp.Dimensions, key)
		}
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// ExpandSources foo
func (s *TSDBStore) ExpandSources(w http.ResponseWriter, r *http.Request) {
	cmd := &ExpandSourcesCommand{}
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Printf("Failed while reading received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = proto.Unmarshal(reqBody, cmd)
	if err != nil {
		log.Printf("Failed while unmarshaling received http body: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	shard := s.Store.Shard(cmd.ShardID)

	var sources influxql.Sources
	err = sources.UnmarshalBinary(cmd.Sources)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := &ExpandSourcesCommandResponse{}
	respSources, err := shard.ExpandSources(sources)
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.Sources, err = respSources.MarshalBinary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}
