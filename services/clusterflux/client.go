package cflux

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// Client is used as a wrapper on meta client to execute
// commands on and read data from meta service cluster.
type Client struct {
	*meta.Client
	// Version tracks current version of the cluster
	dataVersion string
	// mutex is used to lock write (local and to cluster)
	mutex               *sync.Mutex
	logger              *log.Logger
	doneCh              chan struct{}
	errorCh             chan error
	nodePointer         int
	ID                  uint64
	path                string
	ClusterName         string
	ClusterfluxEndpoint string
	BindAddress         string
}

// ClusterResponse used to parse response from Clusterflux
type ClusterResponse struct {
	Body    []byte
	Version string
	Status  int
}

// Metadata struct used to register influxdb node
type Metadata struct {
	IP          string `json:"ip"`
	Hostname    string `json:"hostname"`
	BindAddress string `json:"bindAddress"`
}

// Response used to parse response from Clusterflux for Nodes metadata
type Response struct {
	Status  string   `json:"status"`
	ID      uint64   `json:"id"`
	Exp     string   `json:"expirationDate"`
	Message string   `json:"message"`
	Mdata   Metadata `json:"metadata"`
}

// NodesList is used to return list of nodes
type NodesList struct {
	ID          uint64 `json:"id"`
	IP          string `json:"ip"`
	Hostname    string `json:"hostname"`
	BindAddress string `json:"bindAddress"`
	Alive       bool   `json:"alive"`
}

// Page foo
type Page struct {
	ID uint64 `json:"id"`
}

// NewClient returns a new *Client.
func NewClient(metaConfig *meta.Config, cfluxConfig *Config) *Client {
	return &Client{
		Client:              meta.NewClient(metaConfig),
		dataVersion:         "0",
		mutex:               &sync.Mutex{},
		logger:              log.New(os.Stderr, "[clusterflux] ", log.LstdFlags),
		nodePointer:         0,
		doneCh:              make(chan struct{}),
		errorCh:             make(chan error),
		path:                cfluxConfig.Dir,
		ClusterfluxEndpoint: cfluxConfig.ClusterfluxEndpoint,
		ClusterName:         cfluxConfig.ClusterName,
		BindAddress:         cfluxConfig.BindAddress,
	}
}

// Open a connection to a shard service cluster.
func (c *Client) Open() error {
	err := c.Client.Open()
	if err != nil {
		return err
	}
	// This commented out code could be used to avoid key not found logs during a fresh setup of cluster
	// Commenting it out as it takes a lot of time on kubernetes (works fine on local machine!) Should figure out a better way.
	// _, err = c.postToCflux(c.ClusterName)
	// if err != nil {
	// 	return err
	// }
	c.logger.Println("Setting up node")
	err = c.nodeSetup()
	if err != nil {
		return err
	}
	c.logger.Println("Finished setting up node")

	go c.heartbeat()
	go c.startClusterSync()
	return nil
}

// Close the shard service cluster connection.
func (c *Client) Close() error {
	go c.stopClusterSync()
	return c.Client.Close()
}

// CreateDatabase creates a database or returns it if it already exists
func (c *Client) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	f := func(c *Client) (interface{}, error) {
		return c.Client.CreateDatabase(name)
	}
	val, err := c.ModifyAndSync(f)

	di, ok := val.(*meta.DatabaseInfo)
	if !ok {
		return nil, err
	}
	return di, err
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified retention policy.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, rpi *meta.RetentionPolicyInfo) (*meta.DatabaseInfo, error) {
	f := func(c *Client) (interface{}, error) {
		return c.Client.CreateDatabaseWithRetentionPolicy(name, rpi)
	}
	val, err := c.ModifyAndSync(f)
	di, ok := val.(*meta.DatabaseInfo)
	if !ok {
		return nil, err
	}
	return di, err
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropDatabase(name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	f := func(c *Client) (interface{}, error) {
		return c.Client.CreateRetentionPolicy(database, rpi)
	}
	val, err := c.ModifyAndSync(f)
	rpi, ok := val.(*meta.RetentionPolicyInfo)
	if !ok {
		return nil, err
	}
	return rpi, err
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropRetentionPolicy(database, name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// SetDefaultRetentionPolicy sets a database's default retention policy.
func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.SetDefaultRetentionPolicy(database, name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.UpdateRetentionPolicy(database, name, rpu)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// CreateUser foo
func (c *Client) CreateUser(name, password string, admin bool) (*meta.UserInfo, error) {
	f := func(c *Client) (interface{}, error) {
		return c.Client.CreateUser(name, password, admin)
	}
	val, err := c.ModifyAndSync(f)
	ui, ok := val.(*meta.UserInfo)
	if !ok {
		return nil, err
	}
	return ui, err
}

// UpdateUser foo
func (c *Client) UpdateUser(name, password string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.UpdateUser(name, password)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// DropUser foo
func (c *Client) DropUser(name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropUser(name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// SetPrivilege foo
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.SetPrivilege(username, database, p)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// SetAdminPrivilege foo
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.SetAdminPrivilege(username, admin)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropShard(id)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	f := func(c *Client) (interface{}, error) {
		sgi, err := c.Client.CreateShardGroup(database, policy, timestamp)
		so, err := c.shardOwners(database, policy, sgi)
		if err != nil {
			return nil, err
		}

		sinfos := make([]meta.ShardInfo, len(sgi.Shards))
		for i, shardInfo := range sgi.Shards {
			shardInfo.Owners = so
			sinfos[i] = shardInfo
		}
		sgi.Shards = sinfos
		return sgi, nil
	}
	val, err := c.ModifyAndSync(f)
	sgi, ok := val.(*meta.ShardGroupInfo)
	if !ok {
		return nil, err
	}
	return sgi, err
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DeleteShardGroup(database, policy, id)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	f := func(c *Client) (interface{}, error) {
		err := c.Client.PrecreateShardGroups(from, to)
		if err != nil {
			return nil, err
		}
		for _, di := range c.Client.Data().Databases {
			for _, rp := range di.RetentionPolicies {
				shardGroupInfos, err := c.Client.ShardGroupsByTimeRange(di.Name, rp.Name, from, to)
				if err != nil {
					return nil, err
				}
				for _, sgi := range shardGroupInfos {
					so, err := c.shardOwners(di.Name, rp.Name, &sgi)
					if err != nil {
						return nil, err
					}
					sinfos := make([]meta.ShardInfo, len(sgi.Shards))
					for i, shardInfo := range sgi.Shards {
						shardInfo.Owners = so
						sinfos[i] = shardInfo
					}
					sgi.Shards = sinfos
				}
			}
		}
		return nil, nil
	}
	_, err := c.ModifyAndSync(f)
	return err
}

func (c *Client) shardOwners(database string, policy string, sgi *meta.ShardGroupInfo) ([]meta.ShardOwner, error) {
	rpi, _ := c.Client.RetentionPolicy(database, policy)
	replicaN := rpi.ReplicaN
	shardOwners := make([]meta.ShardOwner, replicaN)
	nodes, _ := c.aliveNodeIDs()
	if len(nodes) < replicaN {
		return nil, errors.New("Repliaction requested is more than the number of available nodes.")
	}
	for i := 0; i < replicaN; i++ {
		shardOwners[i].NodeID = nodes[c.nodePointer]
		c.nodePointer = (c.nodePointer + 1) % len(nodes)
	}
	return shardOwners, nil
}

func (c *Client) aliveNodeIDs() ([]uint64, error) {
	nodeList, err := c.aliveNodes()
	if err != nil {
		return nil, err
	}
	IDs := make([]uint64, 0, len(nodeList))
	for _, node := range nodeList {
		if node.Alive == true {
			IDs = append(IDs, node.ID)
		}
	}
	return IDs, nil
}

func (c *Client) aliveNodes() ([]NodesList, error) {
	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/nodes/" + url.QueryEscape(c.ClusterName)
		return http.NewRequest("GET", url, nil)
	}

	resp, err := c.ExpBackoffRequest(f)
	if err != nil {
		return nil, err
	}
	var nodeList []NodesList
	err = json.NewDecoder(resp.Body).Decode(&nodeList)
	if err != nil {
		return nil, err
	}
	return nodeList, nil
}

// AliveNodesMap foo
func (c *Client) AliveNodesMap() (map[uint64]NodesList, error) {
	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/nodes/" + url.QueryEscape(c.ClusterName)
		return http.NewRequest("GET", url, nil)
	}

	resp, err := c.ExpBackoffRequest(f)
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
	}
	return nodeMap, nil
}

// CreateContinuousQuery foo
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.CreateContinuousQuery(database, name, query)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// DropContinuousQuery foo
func (c *Client) DropContinuousQuery(database, name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropContinuousQuery(database, name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// CreateSubscription foo
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.CreateSubscription(database, rp, name, mode, destinations)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// DropSubscription foo
func (c *Client) DropSubscription(database, rp, name string) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.DropSubscription(database, rp, name)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// SetData foo
func (c *Client) SetData(data *meta.Data) error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.SetData(data)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (c *Client) SetLogOutput(w io.Writer) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.logger = log.New(w, "[clusterflux] ", log.LstdFlags)
	c.Client.SetLogOutput(w)
}

// Load will save the current meta data from disk
func (c *Client) Load() error {
	f := func(c *Client) (interface{}, error) {
		return nil, c.Client.Load()
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// ModifyAndSync foo
func (c *Client) ModifyAndSync(f func(c *Client) (interface{}, error)) (interface{}, error) {
	var val interface{}
	var err error
	c.mutex.Lock()
	defer c.mutex.Unlock()
	lastVersion := c.dataVersion
	lastSnapshot := c.Data()
	for i := 0; i < 5; i++ {
		// apply function that modifies current snapshot
		val, err = f(c)
		if err != nil {
			break
		}
		// send modified snapshot to cflux
		var response ClusterResponse
		c.logger.Println("Posting to Clusterflux")
		response, err = c.postToCflux(c.ClusterName)
		if err != nil {
			break
		}
		// if response is conflict, try to reset local data to receied data
		if response.Status == http.StatusConflict {
			lastSnapshot, err = c.updateData(response)
			if err != nil {
				break
			}
			lastVersion = response.Version
		} else if response.Status == http.StatusOK {
			// success!
			c.logger.Printf("Successfully updated Clusterflux with local changes. Old Version: %s,New Version: %s.", c.dataVersion, response.Version)
			c.dataVersion = response.Version
			return val, nil
		} else {
			// response status is bad (e.g. 500)
			err = errors.New(string(response.Body))
			break
		}
	}
	// rollbak on error
	c.logger.Printf("Failed to update Clusterflux with local changes. Rolling back to version %s.", lastVersion)
	if err2 := c.Client.SetData(&lastSnapshot); err2 != nil {
		c.logger.Println("Failed to rollback.")
		err = errors.New("Multiple errors happened: 1. " + err2.Error() + ", 2. " + err.Error())
	}
	c.dataVersion = lastVersion
	return nil, err
}

// updateData assumes the caller takes care of locks
func (c *Client) updateData(response ClusterResponse) (meta.Data, error) {
	tempData := meta.Data{}
	err := (&tempData).UnmarshalBinary(response.Body)
	if err != nil {
		return meta.Data{}, err
	}
	err = c.Client.SetData(&tempData)
	if err != nil {
		return meta.Data{}, err
	}
	c.logger.Printf("Updated database from version %s to %s", c.dataVersion, response.Version)
	c.dataVersion = response.Version
	return tempData, nil
}

func (c *Client) postToCflux(cluster string) (ClusterResponse, error) {
	cdata := c.Data()
	data, err := (&cdata).MarshalBinary()
	if err != nil {
		return ClusterResponse{}, err
	}

	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/clusters/" + url.QueryEscape(cluster) + "/versions/" + c.dataVersion
		return http.NewRequest("POST", url, bytes.NewBuffer(data))
	}

	resp, err := c.ExpBackoffRequest(f)
	if err != nil {
		return ClusterResponse{}, err
	}
	response, err := c.readResponse(resp)
	if err != nil {
		return ClusterResponse{}, err
	}
	return response, nil
}

func (c *Client) startClusterSync() {
	c.logger.Printf("Started listening for cluster changes.")
	go c.syncWithCluster(c.ClusterName)
	for {
		c.logger.Println(<-c.errorCh)
	}
}

func (c *Client) stopClusterSync() {
	c.logger.Printf("Stopping listening for cluster changes.")
	c.doneCh <- struct{}{}
}

// SyncWithCluster called from server.go to start listening for cluster changes
func (c *Client) syncWithCluster(cluster string) {
	for {
		err := c.sync(cluster)
		if err != nil {
			c.errorCh <- err
		}
		// Need to test if this works. Should work ideally! :P
		select {
		case <-c.doneCh:
			return
		default:
		}
	}
}

func (c *Client) sync(cluster string) error {
	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/clusters/" + url.QueryEscape(cluster) + "/versions/" + c.dataVersion
		return http.NewRequest("GET", url, nil)
	}
	c.logger.Println("Polling for updates from Clusterflux.")
	resp, err := c.ExpBackoffRequest(f)
	if err != nil {
		return err
	}
	response, err := c.readResponse(resp)
	if err != nil {
		return err
	}
	switch response.Status {
	case http.StatusOK:
		c.mutex.Lock()
		_, err = c.updateData(response)
		c.mutex.Unlock()
		if err != nil {
			return err
		}
	case http.StatusInternalServerError:
		return errors.New(string(response.Body))
	case http.StatusGatewayTimeout: // change to 204 No content
		c.logger.Println("Update Timeout: No new updates!")
		return errors.New("No Updates - No new version on remote cluster.")
	}
	return nil
}

// ExpBackoffRequest foo
func (c *Client) ExpBackoffRequest(f func() (*http.Request, error)) (*http.Response, error) {
	client := &http.Client{}
	var resp *http.Response
	var req *http.Request
	var err error

	for attempt := 1; attempt < 6; attempt++ {
		req, err = f()
		if err != nil {
			return nil, err
		}
		resp, err = client.Do(req)
		if err == nil {
			return resp, err
		}
		backoff := (math.Pow(2, float64(attempt)) - 1) / 2
		time.Sleep(time.Duration(backoff) * time.Second)
	}
	c.logger.Printf("Error while connecting to Clusterflux: %s", err.Error())
	return resp, err
}

func (c *Client) readResponse(resp *http.Response) (ClusterResponse, error) {
	status := resp.StatusCode
	version := resp.Header.Get("Version")
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return ClusterResponse{}, err
	}
	response := ClusterResponse{
		Body:    respBody,
		Version: version,
		Status:  status,
	}
	return response, nil
}

func (c *Client) registerNode() error {
	mdata := c.getNodeMetaData()
	data, err := json.Marshal(mdata)
	if err != nil {
		return err
	}
	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/nodes/" + c.ClusterName
		return http.NewRequest("POST", url, bytes.NewBuffer(data))
	}

	resp, err := c.ExpBackoffRequest(f)
	if err != nil {
		c.logger.Println("Failed to Register InfluxDB on Clusterflux")
		return err
	}
	clusterResp := Response{}
	json.NewDecoder(resp.Body).Decode(&clusterResp)
	c.ID = clusterResp.ID
	if err != nil {
		return err
	}
	err = c.writeID()
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) getNodeMetaData() Metadata {
	hostname, err := os.Hostname()
	var addrs []net.IP
	if err == nil {
		addrs, err = net.LookupIP(hostname)
	}
	if err != nil {
		log.Fatalf("Failed to get Hostname/IP.")
	}

	var buffer bytes.Buffer
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			buffer.WriteString(ipv4.String() + ",")
		}
	}
	ip := buffer.String()
	bindaddress := c.BindAddress
	if bindaddress == "" {
		bindaddress = addrs[0].To4().String() + ":8888"
	}
	return Metadata{ip, hostname, bindaddress}
}

func (c *Client) heartbeat() {
	var err error
	for err == nil {
		time.Sleep(5 * time.Second)
		_, err = c.ping()
		if err != nil {
			c.logger.Println("Failed to Register InfluxDB on Clusterflux")
			c.errorCh <- err
		}
	}
}

func (c *Client) ping() (ClusterResponse, error) {
	f := func() (*http.Request, error) {
		url := c.ClusterfluxEndpoint + "/nodes/" + c.ClusterName + "/" + strconv.FormatUint(c.ID, 10)
		return http.NewRequest("PUT", url, nil)
	}

	resp, err := c.ExpBackoffRequest(f)
	response, err := c.readResponse(resp)
	c.logger.Println("response status :", response.Status)
	c.logger.Println("response body :", string(response.Body))
	return response, err
}

func (c *Client) writeID() error {
	p := Page{ID: c.ID}
	j, err := json.Marshal(p)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.path+"/node_id.json", j, 0644)
	if err != nil {
		c.logger.Println("Error while writing to file", err)
		return err
	}
	return nil
}

func (c *Client) nodeID() (uint64, error) {
	raw, err := ioutil.ReadFile(c.path + "/node_id.json")
	if err != nil {
		return 0, err
	}
	var p Page
	json.Unmarshal(raw, &p)
	return p.ID, nil
}

func (c *Client) nodeSetup() error {
	var err error
	c.ID, err = c.nodeID()
	if c.ID == 0 || err != nil {
		c.logger.Println("Previous NodeID not found, registering node and assigning new ID")
		err = c.registerNode()
		if err != nil {
			return err
		}
	}
	resp, err := c.ping()
	if err != nil {
		return err
	}
	if strings.Contains(string(resp.Body), "Key not found") {
		c.logger.Println("Unable to refresh using cached ID, reregistering node and assigning new ID")
		c.registerNode()
	}
	return nil
}
