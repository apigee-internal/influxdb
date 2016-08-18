package cflux

import "errors"

// Config represents the meta configuration.
type Config struct {
	Dir                 string `toml:"dir"`
	ClusterfluxEndpoint string `toml:"clusterflux-endpoint"`
	ClusterName         string `toml:"cluster-name"`
	BindAddress         string `toml:"bind-address"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		ClusterfluxEndpoint: "http://localhost:8000",
		ClusterName:         "default",
		BindAddress:         "http://localhost:8888",
	}
}

// Validate config
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Cflux.Dir must be specified")
	}
	return nil
}
