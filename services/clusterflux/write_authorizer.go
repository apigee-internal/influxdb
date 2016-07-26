package cflux

import "github.com/influxdata/influxdb/services/meta"

type WriteAuthorizer struct {
	*meta.WriteAuthorizer
}

func NewWriteAuthorizer(c *Client) *WriteAuthorizer {
	return &WriteAuthorizer{WriteAuthorizer: meta.NewWriteAuthorizer(c.Client)}
}

// AuthorizeWrite returns nil if the user has permission to write to the database.
func (a WriteAuthorizer) AuthorizeWrite(username, database string) error {
	return a.WriteAuthorizer.AuthorizeWrite(username, database)
}
