package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {
	fmt.Println(logo())
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: RFC3339NanoFixed,
		FullTimestamp:   true,
	})

	cmd, err := newCommand()
	if err != nil {
		logrus.Fatal(err)
		os.Exit(1)
	}

	if err = cmd.Execute(); err != nil {
		logrus.Fatal(err)
		os.Exit(1)
	}
}

func newCommand() (*cobra.Command, error) {
	cfg := NewConfig()
	cmd := &cobra.Command{
		Use:   "nijigen [OPTIONS]",
		Short: "A queue.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return serve(cfg)
		},
		DisableFlagsInUseLine: true,
	}
	cfg.init(cmd.PersistentFlags())
	return cmd, nil
}
