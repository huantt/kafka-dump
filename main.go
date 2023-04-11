package main

import (
	"context"
	_ "embed"
	"encoding/csv"
	"github.com/huantt/kafka-dump/adapter"
	"github.com/huantt/kafka-dump/model"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {
	var logLevel string

	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "", "info", "Log level")
	logConfig := log.Config{Level: logLevel, Format: "text"}
	logConfig.Build()
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

//go:embed whitelist-addresses.txt
var whitelistAddressTxt string

func run(args []string) error {
	whitelistAddress := strings.Split(whitelistAddressTxt, "\n")
	whitelistMap := make(map[string]bool)
	for _, address := range whitelistAddress {
		whitelistMap[address] = true
	}
	ctx := context.Background()
	rest := adapter.NewAnalyticsRest("https://aggregator-api.oxalus.io")
	f, err := os.Open("/Volumes/Data/data.csv")
	if err != nil {
		log.Fatal(err)
	}
	// remember to close the file at the end of the program
	defer f.Close()

	// read csv values using csv.Reader
	batchSize := 100
	concurrent := 5
	csvReader := csv.NewReader(f)
	ch := make(chan model.NftBalance, batchSize*concurrent)
	for i := 0; i < concurrent; i++ {
		go func(threadIndex int) {
			batch := make([]model.NftBalance, 0, batchSize)
			for nftBalance := range ch {
				if len(batch) < batchSize {
					batch = append(batch, nftBalance)
					continue
				}
				err := rest.BulkInsert(ctx, batch)
				if err != nil {
					log.Error(err)
				}
				log.Infof("[%d] Inserted %d - Last ID: %d", threadIndex, len(batch), batch[len(batch)-1].Id)
				batch = make([]model.NftBalance, 0, batchSize)
			}
		}(i)
	}
	for {
		columns, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if strings.TrimSpace(columns[0]) == "id" {
			continue
		}

		// do something with read line
		nftBalance, err := ToNftBalance(columns)
		if err != nil {
			return err
		}
		if !whitelistMap[nftBalance.TokenAddress] {
			log.Debugf("Ignored collection %s", nftBalance.TokenAddress)
			continue
		}

		ch <- *nftBalance
		log.Debugf("%+v\n", columns)
	}
	return nil
}

func ToNftBalance(columns []string) (*model.NftBalance, error) {
	latestBlock, err := strconv.ParseInt(columns[5], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to convert latest block %s", columns[5])
	}
	id, err := strconv.ParseInt(columns[0], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to convert id %s", columns[0])
	}
	return &model.NftBalance{
		Id:           id,
		Chain:        "ethereum",
		Address:      columns[1],
		TokenAddress: columns[2],
		TokenId:      columns[3],
		Amount:       columns[4],
		LatestBlock:  latestBlock,
		EventType:    columns[6],
	}, nil
}
