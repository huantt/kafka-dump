package impl

import (
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type ParquetReader[M any] struct {
	parquetReader *reader.ParquetReader
	fileReader    source.ParquetFile
}

func NewParquetReader[M any](filePath string) (*ParquetReader[M], error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init file reader")
	}

	parquetReader, err := reader.NewParquetReader(fr, new(M), 4)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init parquet reader")
	}
	return &ParquetReader[M]{
		fileReader:    fr,
		parquetReader: parquetReader,
	}, nil
}

const batchSize = 100

func (p *ParquetReader[M]) Read() chan *M {
	rowNum := int(p.parquetReader.GetNumRows())
	ch := make(chan *M, batchSize)
	counter := 0
	go func() {
		for i := 0; i < rowNum/batchSize+1; i++ {
			rows := make([]M, batchSize)
			if err := p.parquetReader.Read(&rows); err != nil {
				err = errors.Wrap(err, "Failed to bulk read messages from parquet file")
				panic(err)
			}

			for _, row := range rows {
				counter++
				log.Debugf("Loaded %d%% (%d/%d)", counter*100/rowNum, counter, rowNum)
				ch <- &row
			}
		}
		p.parquetReader.ReadStop()
		err := p.fileReader.Close()
		if err != nil {
			panic(errors.Wrap(err, "Failed to close fileReader"))
		}
		close(ch)
	}()
	return ch
}
