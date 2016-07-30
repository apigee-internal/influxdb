package cflux

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/spf13/viper"
)

const RETRY_ATTEMPT = 3

// Client is used as a wrapper on meta client to execute
// commands on and read data from meta service cluster.
type Client struct {
	*meta.Client
	// Version tracks current version of the cluster
	dataVersion string
	// mutex is used to lock write (local and to cluster)
	mutex   *sync.Mutex
	logger  *log.Logger
	doneCh  chan struct{}
	errorCh chan error
}

// ClusterResponse used to parse response from Clusterflux
type ClusterResponse struct {
	Body    []byte
	Version string
	Status  int
}

// Metadata struct used to register influxdb node
type Metadata struct {
	IP       string `json:"ip"`
	Hostname string `json:"hostname"`
}

// Response used to parse response from Clusterflux for Nodes metadata
type Response struct {
	Status  string   `json:"status"`
	Id      string   `json:"id"`
	Exp     string   `json:"expirationDate`
	Message string   `json:"message"`
	Mdata   Metadata `json:"metadata"`
}

// NewClient returns a new *Client.
func NewClient(config *meta.Config) *Client {
	viper.SetDefault("CFLUX_ENDPOINT", "http://localhost:8000")
	viper.SetDefault("CLUSTER", "default")
	return &Client{
		Client:      meta.NewClient(config),
		dataVersion: "0",
		mutex:       &sync.Mutex{},
		logger:      log.New(os.Stderr, "[clusterflux] ", log.LstdFlags),
		doneCh:      make(chan struct{}),
		errorCh:     make(chan error),
	}
}

// Open a connection to a shard service cluster.
func (c *Client) Open() error {
	err := c.registerNode()
	if err != nil {
		return err
	}
	go c.startClusterSync()
	return c.Client.Open()
}

// Close the shard service cluster connection.
func (c *Client) Close() error {
	go c.stopClusterSync()
	return c.Client.Close()
}

// AcquireLease attempts to acquire the specified lease.
func (c *Client) AcquireLease(name string) (*meta.Lease, error) {
	return c.Client.AcquireLease(name)
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	return c.Client.ClusterID()
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []meta.DatabaseInfo {
	return c.Client.Databases()
}

// CreateDatabase creates a database or returns it if it already exists
func (c *Client) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	metaOp := func(c *Client) error {
		if _, err := c.Client.CreateDatabase(name); err != nil {
			return err
		}
		return nil
	}
	if err := c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT); err != nil {
		return nil, err
	}
	return c.Database(name), nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified retention policy.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, rpi *meta.RetentionPolicyInfo) (*meta.DatabaseInfo, error) {
	metaOp := func(c *Client) error {
		if _, err := c.Client.CreateDatabaseWithRetentionPolicy(name, rpi); err != nil {
			return err
		}
		return nil
	}
	if err := c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT); err != nil {
		return nil, err
	}
	return c.Database(name), nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropDatabase(name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	metaOp := func(c *Client) error {
		if _, err := c.Client.CreateRetentionPolicy(database, rpi); err != nil {
			return err
		}
		return nil
	}
	if err := c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT); err != nil {
		return nil, err
	}
	return c.RetentionPolicy(database, rpi.Name)
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropRetentionPolicy(database, name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// SetDefaultRetentionPolicy sets a database's default retention policy.
func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.SetDefaultRetentionPolicy(database, name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error {
	metaOp := func(c *Client) error {
		if err := c.Client.UpdateRetentionPolicy(database, name, rpu); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// CreateUser foo
func (c *Client) CreateUser(name, password string, admin bool) (*meta.UserInfo, error) {
	metaOp := func(c *Client) error {
		if _, err := c.Client.CreateUser(name, password, admin); err != nil {
			return err
		}
		return nil
	}
	if err := c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT); err != nil {
		return nil, err
	}
	return c.User(name)
}

// UpdateUser foo
func (c *Client) UpdateUser(name, password string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.UpdateUser(name, password); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// DropUser foo
func (c *Client) DropUser(name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropUser(name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// SetPrivilege foo
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	metaOp := func(c *Client) error {
		if err := c.Client.SetPrivilege(username, database, p); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// SetAdminPrivilege foo
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	metaOp := func(c *Client) error {
		if err := c.Client.SetAdminPrivilege(username, admin); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropShard(id); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	metaOp := func(c *Client) error {
		if _, err := c.Client.CreateShardGroup(database, policy, timestamp); err != nil {
			return err
		}
		return nil
	}
	if err := c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT); err != nil {
		return nil, err
	}
	rpi, err := c.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}

	return rpi.ShardGroupByTimestamp(timestamp), nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DeleteShardGroup(database, policy, id); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	metaOp := func(c *Client) error {
		if err := c.Client.PrecreateShardGroups(from, to); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// CreateContinuousQuery foo
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.CreateContinuousQuery(database, name, query); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// DropContinuousQuery foo
func (c *Client) DropContinuousQuery(database, name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropContinuousQuery(database, name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// CreateSubscription foo
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.CreateSubscription(database, rp, name, mode, destinations); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// DropSubscription foo
func (c *Client) DropSubscription(database, rp, name string) error {
	metaOp := func(c *Client) error {
		if err := c.Client.DropSubscription(database, rp, name); err != nil {
			return err
		}
		return nil
	}
	return c.retryUpdateClusterFluxData(viper.GetString("CLUSTER"), metaOp, RETRY_ATTEMPT)
}

// // SetData foo
// func (c *Client) SetData(data *meta.Data) error {
// 	f := func(c *Client) (interface{}, error) {
// 		return nil, c.Client.SetData(data)
// 	}
// 	_, err := c.ModifyAndSync(f)
// 	return err
// }

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (c *Client) SetLogOutput(w io.Writer) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.logger = log.New(w, "[clusterflux] ", log.LstdFlags)
	c.Client.SetLogOutput(w)
}

// Load will save the current meta data from disk
// TODO: should it call ModifyAndSync? It only updates the cache
func (c *Client) Load() error {
	return c.Client.Load()
}

// metaOperation is a function is wrappper to execute meta operation and return error
type metaOperation func(c *Client) error

func (c *Client) retryUpdateClusterFluxData(cluster string, metaOp metaOperation, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = c.updateCfluxData(cluster, metaOp); err == nil {
			return nil
		}
	}
	return err
}

/*
	updateCfluxData() should be either update cache or rollback to last known snapshot.
					  1) updates clusterflux with latest cached data.
					  2) rollback on error
					  RETRY_ATTEMPT) update to latest and rollback on version conflict
					  4) rollback on non-success HTTP code from clusterflux
*/
func (c *Client) updateCfluxData(cluster string, metaOp metaOperation) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	latestSnapshot := c.Data()
	latestVersion := c.dataVersion
	rollbackOnErr := func(retErr error) error {
		if retErr == nil {
			return nil
		}
		// rollback.
		c.dataVersion = latestVersion
		if err := c.Client.SetData(&latestSnapshot); err != nil {
			return fmt.Errorf("Snapshot rollback failed. Database corrupted(panic???). Original err: %v, Rollback err: %v.", retErr, err)
		}
		return fmt.Errorf("ClusterFlux update failed: %v", retErr)
	}
	// perform meta operation which updates cache.
	err := metaOp(c)
	if err != nil {
		return rollbackOnErr(err)
	}
	// get updated cache.
	cdata := c.Data()
	data, err := (&cdata).MarshalBinary()
	if err != nil {
		return rollbackOnErr(err)
	}
	path := "/clusters/" + url.QueryEscape(cluster) + "/versions/" + c.dataVersion
	resp, err := retryCfluxRequest("POST", path, url.Values{}, data, RETRY_ATTEMPT) // TODO: make attempts configurable
	if err != nil {
		return rollbackOnErr(err)
	}

	// if update on top of latest version. just update version and return.
	if resp.Status == http.StatusOK {
		c.dataVersion = resp.Version
		return nil
	}

	// if version conflict
	// 1) set local data and version to  cflux data and cflux version.
	// 2) error out without rollback. caller retry will update cflux on top of latest snapshot.
	if resp.Status == http.StatusConflict {
		var tempData = meta.Data{}
		err := (&tempData).UnmarshalBinary(data)
		if err != nil {
			return rollbackOnErr(err)
		}
		if err := c.updateCache(resp.Version, tempData); err != nil {
			return rollbackOnErr(err)
		}
		return fmt.Errorf("Version mismatch with ClusterFlux. Current version=%v, ClusterFluxVersion=%v. retrying on top of latest snapshot.", c.dataVersion, resp.Version)
	}
	return rollbackOnErr(fmt.Errorf("HTTP request failed. Status Code:%v, Message:%v", resp.Status, string(resp.Body)))
}

// updateCache updates the local cache data. Caller of this function will lock the update Transaction.
func (c *Client) updateCache(version string, data meta.Data) error {
	err := c.Client.SetData(&data)
	if err != nil {
		return err
	}
	c.logger.Printf("Updated database from version %s to %s", c.dataVersion, version)
	c.dataVersion = version
	return nil
}

func (c *Client) startClusterSync() {
	c.logger.Printf("Started listening for cluster changes.")
	go c.syncWithCluster(viper.GetString("CLUSTER"))
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
	path := "/clusters/" + url.QueryEscape(cluster) + "/versions/" + url.QueryEscape(c.dataVersion)
	c.logger.Println("Polling for updates from Clusterflux.")
	resp, err := retryCfluxRequest("GET", path, url.Values{}, nil, RETRY_ATTEMPT)
	if err != nil {
		return err
	}

	if http.StatusOK == resp.Status {
		var tempData = meta.Data{}
		err := (&tempData).UnmarshalBinary(resp.Body)
		if err != nil {
			return fmt.Errorf("ClusterFlux sync failed:%v", err)
		}
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.updateCache(resp.Version, tempData)
	}
	return fmt.Errorf("ClusterFlux sync failed. HTTPCode: %v, Message: %v", resp.Status, string(resp.Body))
}

func (c *Client) registerNode() error {
	mdata := c.getNodeMetaData()
	data, err := json.Marshal(mdata)
	if err != nil {
		return err
	}
	path := "/nodes/" + viper.GetString("CLUSTER")
	resp, err := retryCfluxRequest("POST", path, url.Values{}, data, RETRY_ATTEMPT)
	if err != nil {
		return fmt.Errorf("Failed to Register InfluxDB on Clusterflux: %v", err)
	}
	if http.StatusOK != resp.Status {
		return fmt.Errorf("Failed to Register InfluxDB on ClusterFlux. HTTPCode: %v, Message:%v", resp.Status, string(resp.Body))
	}
	clusterResp := Response{}
	err = json.Unmarshal(resp.Body, &clusterResp)
	if err != nil {
		return fmt.Errorf("Failed to Register InfluxDB on Clusterflux: %v", err)
	}
	go c.ping(clusterResp.Id)
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
	return Metadata{ip, hostname}
}

func (c *Client) ping(nodeId string) {
	interval := 5
	c5s := time.Tick(time.Second * time.Duration(interval))
	for {
		select {
		case <-c5s:
			path := "/nodes/" + viper.GetString("CLUSTER") + "/" + nodeId
			resp, err := retryCfluxRequest("PUT", path, url.Values{}, nil, RETRY_ATTEMPT)
			if err != nil {
				c.errorCh <- fmt.Errorf("Failed to refresh InfluxDB Data node to ClusterFlux: %v", err)
			}
			if http.StatusOK != resp.Status {
				c.errorCh <- fmt.Errorf("Failed to refresh InfluxDB Data node to ClusterFlux. HTTPCode: %v, Message:%v", resp.Status, string(resp.Body))
			}
		}
	}
}

// retryCfluxRequest makes HTTP request to clustrflux with expontional backoff sleep interval for no of attempts
func retryCfluxRequest(method, path string, params url.Values, body []byte, attempts int) (ClusterResponse, error) {
	var resp = ClusterResponse{}
	var err error
	for attempt := 1; attempt < attempt; attempt++ {
		resp, err = sendCfluxRequest(method, path, params, body)
		if err == nil {
			return resp, err
		}
		backoff := (math.Pow(2, float64(attempt)) - 1) / 2
		time.Sleep(time.Duration(backoff) * time.Second)
	}
	return resp, err
}

// sendCFluxRequst makes HTTP request to clusterflux APIs.
func sendCfluxRequest(method, path string, params url.Values, body []byte) (ClusterResponse, error) {
	url := viper.GetString("CFLUX_ENDPOINT") + path + "?" + params.Encode()
	c := &http.Client{
		Timeout: time.Second * 5, // TODO: make it configurable.
	}
	var resp = ClusterResponse{}
	var err error

	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return resp, err
	}

	httpResp, err := c.Do(req)
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()
	resp.Version = httpResp.Header.Get("Version")
	resp.Status = httpResp.StatusCode

	data, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return resp, err
	}
	resp.Body = data
	return resp, err
}
