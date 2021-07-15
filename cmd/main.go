package main

import (
	"encoding/base64"
	"fmt"

	"github.com/jylc/nijigen-queue/internal/env"
	"github.com/jylc/nijigen-queue/internal/logger"
	"github.com/spf13/cobra"
)

var (
	envv = ""

	rootCmd = &cobra.Command{
		Use:   "nijigen",
		Short: "nijigen queue is a nijigen queue",
	}

	runCmd = &cobra.Command{
		Use:     "run",
		Short:   "Run the nijigen queue",
		Long:    "Run the nijigen queue",
		Run:     run,
		Example: "nijigen run --env dev",
	}
)

func main() {
	initCmd()

	if err := rootCmd.Execute(); err != nil {
		logger.Fatal([]byte(err.Error()))
	}
}

func initCmd() {
	rootCmd.SetHelpTemplate(logo() + "\n\n" + rootCmd.HelpTemplate())
	rootCmd.PersistentFlags().StringVarP(&envv, "env", "", env.Dev, "to decide program run with which env")
	rootCmd.AddCommand(runCmd)
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func run(cmd *cobra.Command, args []string) {
	fmt.Println(logo())

	env.Set(envv)

	// TODO
}

const encodedLogo = "ICBfICAgXyBfX18gICAgXyBfX18gX19fXyBfX19fXyBfICAgXyAKIHwgXCB8IHxfIF98ICB8IHxfIF8vIF9fX3wgX19fX3wgXCB8IHwKIHwgIFx8IHx8IHxfICB8IHx8IHwgfCAgX3wgIF98IHwgIFx8IHwKIHwgfFwgIHx8IHwgfF98IHx8IHwgfF98IHwgfF9fX3wgfFwgIHwKIHxffCBcX3xfX19cX19fL3xfX19cX19fX3xfX19fX3xffCBcX3wKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA="

func logo() string {
	res, _ := base64.StdEncoding.DecodeString(encodedLogo)
	return string(res)
}
