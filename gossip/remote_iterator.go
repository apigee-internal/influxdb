package gossip

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/spf13/viper"
)

// RemoteIteratorCreator implements influxql.IteratorCreator
type RemoteIteratorCreator struct {
	NodeID   uint64
	ShardIDs []uint64
}

// NodesList is used to return list of nodes
type NodesList struct {
	ID       uint64 `json:"id"`
	IP       string `json:"ip"`
	Hostname string `json:"hostname"`
	BindAddr string `json:"bind-address"`
	Alive    bool   `json:"alive"`
}

// CreateIterator Creates a simple iterator for use in an InfluxQL query for the remote node
func (ric *RemoteIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	aliveNodes, err := AliveNodesMap()
	log.Printf("************* NodeID = %d, aliveNodes = %v", ric.NodeID, aliveNodes[ric.NodeID])
	url := "http://" + aliveNodes[ric.NodeID].BindAddr + "/read"
	cmd := &ReadShardCommand{
		ShardIDs:  ric.ShardIDs,
		StartTime: &opt.StartTime,
	}
	data, err := proto.Marshal(cmd)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(data))
	_, err = ExpBackoffRequest(*req)
	if err != nil {
		log.Printf("Failed to read shards from remote node with ID: %d", ric.NodeID)
		return nil, err

		// time.Parse(time.RFC3339Nano, s)
	}
	return nil, nil

}

// FieldDimensions Returns the unique fields and dimensions across a list of sources from the remote node
func (ric *RemoteIteratorCreator) FieldDimensions(sources influxql.Sources) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

// ExpandSources Expands regex sources to all matching sources for the remote nodes
func (ric *RemoteIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return nil, nil
}

// AliveNodesMap foo
func AliveNodesMap() (map[uint64]NodesList, error) {
	url := viper.GetString("CFLUX_ENDPOINT") + "/nodes/" + url.QueryEscape(viper.GetString("CLUSTER"))
	req, err := http.NewRequest("GET", url, nil)
	resp, err := ExpBackoffRequest(*req)
	if err != nil {
		return nil, err
	}
	var nodeList []NodesList
	nodeMap := map[uint64]NodesList{}
	err = json.NewDecoder(resp.Body).Decode(&nodeList)
	if err != nil {
		return nil, err
	}
	for _, node := range nodeList {
		nodeMap[node.ID] = node
		log.Printf("***** assign alive to %d = %v", node.ID, node)
	}
	return nodeMap, nil
}

// ExpBackoffRequest foo
func ExpBackoffRequest(req http.Request) (*http.Response, error) {
	client := &http.Client{}
	var resp *http.Response
	var err error

	for attempt := 1; attempt < 6; attempt++ {
		resp, err = client.Do(&req)
		if err == nil {
			return resp, err
		}
		backoff := (math.Pow(2, float64(attempt)) - 1) / 2
		time.Sleep(time.Duration(backoff) * time.Second)
	}
	log.Printf("Error while connecting to Clusterflux: %s", err.Error())
	return resp, err
}
