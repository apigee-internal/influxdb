package cflux

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/spf13/viper"
)

// Client is used as a wrapper on meta client to execute
// commands on and read data from meta service cluster.
type Client struct {
	*meta.Client
	// Version tracks current version of the cluster
	dataVersion string
	// mutex is used to lock write (local and to cluster)
	mutex *sync.Mutex
}

type ClusterResponse struct {
	Body    []byte
	Version string
	Status  int
}

// NewClient returns a new *Client.
func NewClient(config *meta.Config) *Client {
	viper.SetDefault("CFLUX_ENDPOINT", "http://localhost:8000")
	viper.SetDefault("CLUSTER", "default")
	return &Client{
		Client:      meta.NewClient(config),
		dataVersion: "0",
		mutex:       &sync.Mutex{},
	}
}

// Open a connection to a shard service cluster.
func (c *Client) Open() error {
	return c.Client.Open()
}

// Close the shard service cluster connection.
func (c *Client) Close() error {
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

// Database returns info for the requested database.
func (c *Client) Database(name string) *meta.DatabaseInfo {
	return c.Client.Database(name)
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []meta.DatabaseInfo {
	return c.Client.Databases()
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

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return c.Client.RetentionPolicy(database, name)
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

// Users foo
func (c *Client) Users() []meta.UserInfo {
	return c.Client.Users()
}

// User foo
func (c *Client) User(name string) (*meta.UserInfo, error) {
	return c.Client.User(name)
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

// UserPrivileges foo
func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return c.Client.UserPrivileges(username)
}

// UserPrivilege foo
func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return c.Client.UserPrivilege(username, database)
}

// AdminUserExists foo
func (c *Client) AdminUserExists() bool {
	return c.Client.AdminUserExists()
}

// Authenticate foo
func (c *Client) Authenticate(username, password string) (*meta.UserInfo, error) {
	return c.Client.Authenticate(username, password)
}

// UserCount foo
func (c *Client) UserCount() int {
	return c.Client.UserCount()
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	return c.Client.ShardIDs()
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return c.Client.ShardGroupsByTimeRange(database, policy, min, max)
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	return c.Client.ShardsByTimeRange(sources, tmin, tmax)
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
		return c.Client.CreateShardGroup(database, policy, timestamp)
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
		return nil, c.Client.PrecreateShardGroups(from, to)
	}
	_, err := c.ModifyAndSync(f)
	return err
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return c.Client.ShardOwner(shardID)
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

// Data foo
func (c *Client) Data() meta.Data {
	return c.Client.Data()
}

// WaitForDataChanged will return a channel that will get closed when
// the metastore data has changed
func (c *Client) WaitForDataChanged() chan struct{} {
	return c.Client.WaitForDataChanged()
}

// MarshalBinary foo
func (c *Client) MarshalBinary() ([]byte, error) {
	return c.Client.MarshalBinary()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (c *Client) SetLogOutput(w io.Writer) {
	c.Client.SetLogOutput(w)
}

// Load will save the current meta data from disk
// TODO: should it call ModifyAndSync? It only updates the cache
func (c *Client) Load() error {
	return c.Client.Load()
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
			return nil, err
		}

		// send modified snapshot to cflux
		var response ClusterResponse
		response, err = c.postToCflux(viper.GetString("CLUSTER"))
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
			c.dataVersion = response.Version
			return val, nil
		} else {
			// response status is bad (e.g. 500)
			err = errors.New(string(response.Body))
			break
		}
	}
	// rollbak on error
	if err2 := c.Client.SetData(&lastSnapshot); err2 != nil {
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
	// c.mutex.Lock()
	err = c.Client.SetData(&tempData)
	// c.mutex.Unlock()
	if err != nil {
		return meta.Data{}, err
	}
	c.dataVersion = response.Version
	return tempData, nil
}

func (c *Client) postToCflux(cluster string) (ClusterResponse, error) {
	url := viper.GetString("CFLUX_ENDPOINT") + "/clusters/" + url.QueryEscape(cluster) + "/versions/" + c.dataVersion
	cdata := c.Data()
	data, err := (&cdata).MarshalBinary()
	if err != nil {
		return ClusterResponse{}, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	resp, err := c.expBackoffRequest(cluster, *req)
	if err != nil {
		return ClusterResponse{}, err
	}
	response, err := c.readResponse(resp)
	if err != nil {
		return ClusterResponse{}, err
	}
	return response, nil
}

// SyncWithCluster called from server.go to start listening for cluster changes
func (c *Client) SyncWithCluster(cluster string, done chan struct{}, ch chan error) {
	for {
		err := c.sync(cluster)
		if err != nil {
			ch <- err
		}
		// Need to test if this works. Should work ideally! :P
		select {
		case <-done:
			return
		default:
		}
	}
}

func (c *Client) sync(cluster string) error {
	url := viper.GetString("CFLUX_ENDPOINT") + "/clusters/" + url.QueryEscape(cluster) + "/versions/" + c.dataVersion
	req, err := http.NewRequest("GET", url, nil)

	resp, err := c.expBackoffRequest(cluster, *req)
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
	case http.StatusGatewayTimeout:
		return errors.New("No Updates - No new version on remote cluster.")
	}
	return nil
}

func (c *Client) expBackoffRequest(cluster string, req http.Request) (*http.Response, error) {
	client := &http.Client{}
	var resp *http.Response
	var err error

	for attempt := 1; attempt < 6; attempt++ {
		resp, err = client.Do(&req)
		if err == nil {
			break
		}
		backoff := (math.Pow(2, float64(attempt)) - 1) / 2
		time.Sleep(time.Duration(backoff) * time.Second)
	}
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
