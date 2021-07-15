package main

import (
	"github.com/spf13/pflag"
)

// RFC3339NanoFixed is time.RFC3339Nano with nanoseconds padded using zeros to
// ensure the formatted time isalways the same number of characters.
const RFC3339NanoFixed = "2006-01-02T15:04:05.000000000Z07:00"

type Config struct {
	port     string
	Debug    bool
	LogLevel string
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) init(flags *pflag.FlagSet) {
	flags.StringVarP(&c.port, "port", "p", "6789", "set listening port")
	flags.BoolVarP(&c.Debug, "debug", "d", false, "enable debug mode")
	flags.StringVarP(&c.LogLevel, "log", "l", "info", `set the logging level ("debug"|"info"|"warn"|"error"|"fatal")`)
	// TODO config file
}
