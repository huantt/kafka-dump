package cmd

import (
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func CreateCountParquetRowCommand() (*cobra.Command, error) {
	var filePath string

	command := cobra.Command{
		Use: "count-parquet-rows",
		Run: func(cmd *cobra.Command, args []string) {
			parquetReader, err := impl.NewParquetReader(filePath, false)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}
			log.Infof("Number of rows: %d", parquetReader.GetNumberOfRows())
		},
	}
	command.Flags().StringVarP(&filePath, "file", "f", "", "File path (required)")
	err := command.MarkFlagRequired("file")
	if err != nil {
		return nil, err
	}
	return &command, nil
}
