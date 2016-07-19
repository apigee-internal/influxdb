package cflux

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/spf13/viper"
)

// VERSION tracks current version of the cluster
var VERSION int
var FAIL_ATTEMPTS int

// Client is used as a wrapper on meta client to execute
// commands on and read data from meta service cluster.
type Client struct {
	*meta.Client
}

// NewClient returns a new *Client.
func NewClient(config *meta.Config) *Client {
	viper.SetDefault("CFLUX_ENDPOINT", "localhost:8000")
	viper.SetDefault("CLUSTER", "default")
	VERSION = 0
	FAIL_ATTEMPTS = 0
	return &Client{
		meta.NewClient(config),
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
	db, err := c.Client.CreateDatabase(name)
	if err != nil {
		return db, err
	}
	_, err = c.postToCflux(viper.GetString("CLUSTER"), name)
	if err != nil {
		if strings.Contains(err.Error(), "Retry") {
			c.CreateDatabase(name)
		}
		// should I delete the created database or retry? DB could be inconsistent
		c.Client.DropDatabase(name)
	}
	return db, err
}

// pass in function. copy Data, do operation, revert to snapshot on failure
// use defer
// goto err handler
func (c *Client) postToCflux(cluster string, dbName string) (ClusterResponse, error) {
	var req *http.Response
	cdata := c.Data()
	response := &ClusterResponse{}
	// if err return err
	// revert to old snapshot and return err
	data, err := (&cdata).MarshalBinary()
	if err != nil {
		if FAIL_ATTEMPTS < 5 {
			FAIL_ATTEMPTS = FAIL_ATTEMPTS + 1
			c.postToCflux(cluster, dbName)
		}
		FAIL_ATTEMPTS = 0
		// Should/Shouldn't we drop the local copy if failed to put in cluster.
		// move it to the caller (independent of who is calling)
		c.Client.DropDatabase(dbName)
		return *response, err
	}
	url := viper.GetString("CFLUX_ENDPOINT") + "/" + cluster + "/" + strconv.Itoa(VERSION)
	// think fn programming
	for attempt := 1; attempt < 6; attempt++ {
		req, err = http.Post(url, "", bytes.NewBuffer(data))
		if err == nil {
			break
		}
		backoff := (math.Pow(2, float64(attempt)) - 1) / 2
		time.Sleep(time.Duration(backoff) * time.Second)
	}
	if err != nil {
		// Should/Shouldn't we drop the local copy if failed to put in cluster.
		c.Client.DropDatabase(dbName)
		return *response, errors.New("Error in posting to clusterflux. " + err.Error())
	}
	resp, err := ioutil.ReadAll(req.Body)
	if err != nil {
		// revert to snapshot and err out
		c.Client.DropDatabase(dbName)
		return *response, errors.New("ioutil.ReadAll failed: " + err.Error())
		// retry?
	}
	err = proto.Unmarshal(resp, response)
	if err != nil {
		c.Client.DropDatabase(dbName)
		return *response, errors.New("Failed unmarshaling response from cluster: " + err.Error())
		// return *response, errors.New("Retry - prooto.unmarshal failed: " + err.Error())
	}
	ver, err := strconv.Atoi(response.GetVersion())
	if err != nil {
		// revert to snapshot and err out
		c.Client.DropDatabase(dbName)
		return *response, errors.New("Failed while converting string to integer: " + err.Error())
	}
	if !*response.Success {
		if ver > VERSION {
			err = (&cdata).UnmarshalBinary([]byte(response.GetData()))
			if err != nil {
				c.Client.DropDatabase(dbName)
				return *response, errors.New("Failed unmarshaling response Data from cluster: " + err.Error())
			}
			return *response, errors.New("Retry on the updated Data")
		}
		c.Client.DropDatabase(dbName)
		return *response, errors.New(response.GetMessage())
	}
	VERSION = ver
	return *response, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified retention policy.
// TODO: protobuf
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, rpi *meta.RetentionPolicyInfo) (*meta.DatabaseInfo, error) {
	return c.Client.CreateDatabaseWithRetentionPolicy(name, rpi)
}

// DropDatabase deletes a database.
// TODO: protobuf
func (c *Client) DropDatabase(name string) error {
	return c.Client.DropDatabase(name)
}

// CreateRetentionPolicy creates a retention policy on the specified database.
// TODO: protobuf
func (c *Client) CreateRetentionPolicy(database string, rpi *meta.RetentionPolicyInfo) (*meta.RetentionPolicyInfo, error) {
	return c.Client.CreateRetentionPolicy(database, rpi)
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return c.Client.RetentionPolicy(database, name)
}

// DropRetentionPolicy drops a retention policy from a database.
// TODO: protobuf
func (c *Client) DropRetentionPolicy(database, name string) error {
	return c.Client.DropRetentionPolicy(database, name)
}

// SetDefaultRetentionPolicy sets a database's default retention policy.
// TODO: protobuf
func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	return c.Client.SetDefaultRetentionPolicy(database, name)
}

// UpdateRetentionPolicy updates a retention policy.
// TODO: protobuf
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate) error {
	return c.Client.UpdateRetentionPolicy(database, name, rpu)
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
// TODO: protobuf
func (c *Client) CreateUser(name, password string, admin bool) (*meta.UserInfo, error) {
	return c.Client.CreateUser(name, password, admin)
}

// UpdateUser foo
// TODO: protobuf
func (c *Client) UpdateUser(name, password string) error {
	return c.Client.UpdateUser(name, password)
}

// DropUser foo
// TODO: protobuf
func (c *Client) DropUser(name string) error {
	return c.Client.DropUser(name)
}

// SetPrivilege foo
// TODO: protobuf
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	return c.Client.SetPrivilege(username, database, p)
}

// SetAdminPrivilege foo
// TODO: protobuf
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	return c.Client.SetAdminPrivilege(username, admin)
}

// UserPrivileges foo
func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return c.UserPrivileges(username)
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
// TODO: protobuf
func (c *Client) DropShard(id uint64) error {
	return c.Client.DropShard(id)
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
// TODO: protobuf
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return c.Client.CreateShardGroup(database, policy, timestamp)
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
// TODO: protobuf
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	return c.Client.DeleteShardGroup(database, policy, id)
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
// TODO: protobuf
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	return c.Client.PrecreateShardGroups(from, to)
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo) {
	return c.Client.ShardOwner(shardID)
}

// CreateContinuousQuery foo
// TODO: protobuf (might)
func (c *Client) CreateContinuousQuery(database, name, query string) error {
	return c.Client.CreateContinuousQuery(database, name, query)
}

// DropContinuousQuery foo
// TODO: protobuf (might)
func (c *Client) DropContinuousQuery(database, name string) error {
	return c.Client.DropContinuousQuery(database, name)
}

// CreateSubscription foo
// TODO: protobuf (might)
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return c.Client.CreateSubscription(database, rp, name, mode, destinations)
}

// DropSubscription foo
// TODO: protobuf (might)
func (c *Client) DropSubscription(database, rp, name string) error {
	return c.Client.DropSubscription(database, rp, name)
}

// SetData foo
// TODO: protobuf (might)
func (c *Client) SetData(data *meta.Data) error {
	return c.Client.SetData(data)
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
// TODO: protobuf (might)
func (c *Client) SetLogOutput(w io.Writer) {
	c.Client.SetLogOutput(w)
}

// Load will save the current meta data from disk
// TODO: protobuf (might)
func (c *Client) Load() error {
	return c.Client.Load()
}
