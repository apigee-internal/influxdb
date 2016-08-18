package gossip

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
)

// RemoteIteratorCreator implements influxql.IteratorCreator
type RemoteIteratorCreator struct {
	Store   *TSDBStore
	ShardID uint64
	NodeID  uint64
}

// NodesList is used to return list of nodes
type NodesList struct {
	ID          uint64 `json:"id"`
	IP          string `json:"ip"`
	Hostname    string `json:"hostname"`
	BindAddress string `json:"bindAddress"`
	Alive       bool   `json:"alive"`
}

// CreateIterator Creates a simple iterator for use in an InfluxQL query for the remote node
func (ric *RemoteIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	aliveNodes, err := ric.Store.Client.AliveNodesMap()
	optBinary, err := opt.MarshalBinary()
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while marshaling IteratorOptions: %s", err.Error())
		return nil, err
	}

	cmd := &ReadShardCommand{
		ShardID:         ric.ShardID,
		IteratorOptions: optBinary,
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while marshaling ReadShardCommand: %s", err.Error())
		return nil, err
	}

	f := func() (*http.Request, error) {
		url := "http://" + aliveNodes[ric.NodeID].BindAddress + "/read"
		return http.NewRequest("POST", url, bytes.NewBuffer(data))
	}

	resp, err := ric.Store.Client.ExpBackoffRequest(f)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Failed to read shards from remote node with ID: %d", ric.NodeID)
		return nil, err
	}

	respMessage := &ReadShardCommandResponse{}
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Failed while reading received http body: %s", err.Error())
		return nil, err
	}
	err = proto.Unmarshal(respBody, respMessage)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while unmarshaling response: %s", err.Error())
		return nil, err
	}

	dec := influxql.NewPointDecoder(bytes.NewReader(respMessage.Points))
	var iter influxql.Iterator
	iterType := respMessage.Type
	switch iterType {
	case ReadShardCommandResponse_FLOAT:
		iter = &RemoteFloatIterator{PointDecoder: dec, Closed: false}
	case ReadShardCommandResponse_INTEGER:
		iter = &RemoteIntegerIterator{PointDecoder: dec, Closed: false}
	case ReadShardCommandResponse_STRING:
		iter = &RemoteStringIterator{PointDecoder: dec, Closed: false}
	case ReadShardCommandResponse_BOOLEAN:
		iter = &RemoteBooleanIterator{PointDecoder: dec, Closed: false}
	default:
		return nil, fmt.Errorf("[RemoteIteratorCreator] Unsupported iterator type: %d", iterType)
	}

	return iter, nil
}

// FieldDimensions Returns the unique fields and dimensions across a list of sources from the remote node
func (ric *RemoteIteratorCreator) FieldDimensions(sources influxql.Sources) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	sourceBinary, err := sources.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}
	fdc := &FieldDimensionsCommand{ShardID: ric.ShardID, Sources: sourceBinary}
	fdcBinary, err := proto.Marshal(fdc)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while marshaling FieldDimensionsCommand: %s", err.Error())
		return nil, nil, err
	}

	aliveNodes, err := ric.Store.Client.AliveNodesMap()
	if err != nil {
		return nil, nil, err
	}
	f := func() (*http.Request, error) {
		url := "http://" + aliveNodes[ric.NodeID].BindAddress + "/fielddimensions"
		return http.NewRequest("POST", url, bytes.NewBuffer(fdcBinary))
	}

	resp, err := ric.Store.Client.ExpBackoffRequest(f)
	if err != nil {
		return nil, nil, err
	}

	respMessage := &FieldDimensionsCommandResponse{}
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Failed while reading received http body: %s", err.Error())
		return nil, nil, err
	}
	err = proto.Unmarshal(respBody, respMessage)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while unmarshaling response: %s", err.Error())
		return nil, nil, err
	}

	if respMessage.Error != "" {
		return nil, nil, errors.New(respMessage.Error)
	}

	fields = make(map[string]influxql.DataType)
	for key, value := range respMessage.GetFields() {
		fields[key] = influxql.DataType(value)
	}

	dimensions = make(map[string]struct{})
	for _, dimension := range respMessage.Dimensions {
		dimensions[dimension] = struct{}{}
	}

	return fields, dimensions, nil
}

// ExpandSources Expands regex sources to all matching sources for the remote nodes
func (ric *RemoteIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	sourceBinary, err := sources.MarshalBinary()
	if err != nil {
		return nil, err
	}
	cmd := &ExpandSourcesCommand{ShardID: ric.ShardID, Sources: sourceBinary}
	cmdBinary, err := proto.Marshal(cmd)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while marshaling ExpandSourcesCommand: %s", err.Error())
		return nil, err
	}

	aliveNodes, err := ric.Store.Client.AliveNodesMap()
	if err != nil {
		return nil, err
	}

	f := func() (*http.Request, error) {
		url := "http://" + aliveNodes[ric.NodeID].BindAddress + "/expandsources"
		return http.NewRequest("POST", url, bytes.NewBuffer(cmdBinary))
	}
	resp, err := ric.Store.Client.ExpBackoffRequest(f)
	if err != nil {
		return nil, err
	}

	respMessage := &ExpandSourcesCommandResponse{}
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Failed while reading received http body: %s", err.Error())
		return nil, err
	}
	err = proto.Unmarshal(respBody, respMessage)
	if err != nil {
		log.Printf("[RemoteIteratorCreator] Error while unmarshaling response: %s", err.Error())
		return nil, err
	}
	if respMessage.Error != "" {
		return nil, errors.New(respMessage.Error)
	}
	var respSources influxql.Sources
	err = respSources.UnmarshalBinary(respMessage.Sources)
	if err != nil {
		return nil, err
	}

	return respSources, nil
}
