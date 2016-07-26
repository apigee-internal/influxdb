package cflux

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

type QueryAuthorizer struct {
	*meta.QueryAuthorizer
}

func NewQueryAuthorizer(c *Client) *QueryAuthorizer {
	return &QueryAuthorizer{
		QueryAuthorizer: meta.NewQueryAuthorizer(c.Client),
	}
}

// AuthorizeQuery authorizes u to execute q on database.
// Database can be "" for queries that do not require a database.
// If no user is provided it will return an error unless the query's first statement is to create
// a root user.
func (a *QueryAuthorizer) AuthorizeQuery(u *meta.UserInfo, query *influxql.Query, database string) error {
	return a.QueryAuthorizer.AuthorizeQuery(u, query, database)
}

// // TODO: check if this is required
// // Error returns the text of the error.
// func (e meta.ErrAuthorize) Error() string {
// 	if e.User == "" {
// 		return fmt.Sprint(e.Message)
// 	}
// 	return fmt.Sprintf("%s not authorized to execute %s", e.User, e.Message)
// }
