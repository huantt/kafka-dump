package main

import (
	"github.com/huantt/kafka-dump/cmd"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
)

func main() {
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	var logLevel string

	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "", "info", "Log level")
	logConfig := log.Config{Level: logLevel, Format: "text"}
	logConfig.Build()

	exportCommand, err := cmd.CreateExportCommand()
	if err != nil {
		return errors.Wrap(err, "Failed to create export command")
	}
	importCommand, err := cmd.CreateImportCmd()
	if err != nil {
		return errors.Wrap(err, "Failed to create import command")
	}

	streamCommand, err := cmd.CreateStreamCmd()
	if err != nil {
		return errors.Wrap(err, "Failed to create stream command")
	}

	countParquetNumberOfRowsCmd, err := cmd.CreateCountParquetRowCommand()
	if err != nil {
		return errors.Wrap(err, "Failed to create parquet row counter command")
	}

	rootCmd.AddCommand(
		exportCommand,
		importCommand,
		streamCommand,
		countParquetNumberOfRowsCmd,
	)

	return rootCmd.Execute()
}
