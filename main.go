package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-kit/kit/log"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/prometheus/documentation/examples/custom-sd/adapter"
)

const (
	sdName = "RDSSD"
)

var (
	a               = kingpin.New("sd adapter usage", "Tool to generate file_sd target files for AWS RDS SD .")
	outputFile      = a.Flag("output.file", "Output file for file_sd compatible file.").Default("rds_sd.json").String()
	refreshInterval = a.Flag("refresh.interval", "Refresh interval to re-read the instance list.").Default("60").Int()
	logger          log.Logger
)

type sdConfig struct {
	RefreshInterval int
}

func main() {
	a.HelpFlag.Short('h')

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Println("err: ", err)
		return
	}

	logger = log.NewSyncLogger(log.NewLogfmtLogger(os.Stdout))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	ctx := context.Background()

	cfg := sdConfig{
		RefreshInterval: *refreshInterval,
	}

	disc, err := newDiscovery(cfg, logger)
	sdAdapter := adapter.NewAdapter(ctx, *outputFile, sdName, disc, logger)
	sdAdapter.Run()

	<-ctx.Done()
}
