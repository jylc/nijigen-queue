package main

import (
	"fmt"

	"github.com/panjf2000/gnet"
	"github.com/sirupsen/logrus"

	"github.com/jylc/nijigen-queue/internal/decoder"
)

func run(conf *Config) error {
	if err := configLogger(conf); err != nil {
		return err
	}

	logrus.Fatal(gnet.Serve(
		&Server{},
		fmt.Sprintf("tcp://0.0.0.0:%s", conf.port),
		gnet.WithMulticore(true),
		gnet.WithCodec(&decoder.MessageDecoder{}),
	))
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
