package cmd

import (
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func CreateCountParquetRowCommand() (*cobra.Command, error) {
	var filePathMessage string
	var filePathOffset string

	command := cobra.Command{
		Use: "count-parquet-rows",
		Run: func(cmd *cobra.Command, args []string) {
			parquetReader, err := impl.NewParquetReader(filePathMessage, filePathOffset, false)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}
			log.Infof("Number of rows in message file: %d", parquetReader.GetNumberOfRowsInMessageFile())
			log.Infof("Number of rows in offset file: %d", parquetReader.GetNumberOfRowsInOffsetFile())
		},
	}
	command.Flags().StringVarP(&filePathMessage, "file", "f", "", "File path of stored kafka message (required)")
	command.Flags().StringVarP(&filePathOffset, "offset-file", "o", "", "File path of stored kafka offset (required)")
	err := command.MarkFlagRequired("file")
	if err != nil {
		return nil, err
	}
	err = command.MarkFlagRequired("offset-file")
	if err != nil {
		return nil, err
	}
	return &command, nil
}
