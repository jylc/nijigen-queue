package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func run(conf *Config) error {
	if err := configLogger(conf); err != nil {
		return err
	}

	// TODO listening tcp on conf.port
	return nil
}

func configLogger(conf *Config) error {
	if conf.LogLevel != "" {
		lvl, err := logrus.ParseLevel(conf.LogLevel)
		if err != nil {
			return fmt.Errorf("unable to parse logging level: %s", conf.LogLevel)
		}
		logrus.SetLevel(lvl)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: RFC3339NanoFixed,
		DisableColors:   false,
		FullTimestamp:   true,
	})
	return nil
}
