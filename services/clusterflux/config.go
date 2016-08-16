package cflux

import "errors"

// Config represents the meta configuration.
type Config struct {
	Dir           string `toml:"dir"`
	CfluxEndpoint string `toml:"cfluxEndpoint"`
	Cluster       string `toml:"cluster"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		CfluxEndpoint: "http://localhost:8000",
		Cluster:       "default",
	}
}

// Validate config
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Cflux.Dir must be specified")
	}
	return nil
}
