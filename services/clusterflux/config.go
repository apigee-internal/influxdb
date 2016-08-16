package cflux

// Config represents the meta configuration.
type Config struct {
	Dir            string `toml:"dir"`
	CFLUX_ENDPOINT string `toml:"cfluxEndpoint"`
	CLUSTER        string `toml:"cluster"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		CFLUX_ENDPOINT: "http://localhost:8000",
		CLUSTER:        "default",
	}
}
