package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/cenkalti/backoff/v4"
)

var version = "dev"

var (
	optDatabase       = flag.String("database", "", "database")
	optQuery          = flag.String("query", "", "path/to/query.txt")
	optStat           = flag.String("stat", "/dev/stderr", "output last stat")
	optOut            = flag.String("out", "/dev/stdout", "path/to/result/file")
	optVersion        = flag.Bool("version", false, "show version")
	optTSV            = flag.Bool("tsv", false, "tsv")
	optOutputLocation = flag.String("output-location", "", "s3://bucket/key/to/out/")
	optWorkGroup      = flag.String("work-group", "", "name of workgroup")
)

func main() {
	flag.Parse()

	if *optVersion {
		fmt.Println(version)
		return
	}

	if err := run(); err != nil {
		log.Fatalf("*** %v", err)
	}
}

func run() error {

	if *optQuery == "" {
		return fmt.Errorf("--query must be specified")
	}

	fp, err := os.Create(*optOut)
	if err != nil {
		return fmt.Errorf("os.Create out --out: %v", err)
	}
	defer fp.Close()

	b, err := os.ReadFile(*optQuery)
	if err != nil {
		return fmt.Errorf("os.ReadFile: %v", err)
	}

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		return fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	cl := athena.NewFromConfig(cfg)

	var resultConf *types.ResultConfiguration
	if *optOutputLocation != "" {
		resultConf = &types.ResultConfiguration{
			OutputLocation: aws.String(*optOutputLocation),
		}
	}

	var workGroup *string
	if *optWorkGroup != "" {
		workGroup = optWorkGroup
	}

	var queryExecutionContext *types.QueryExecutionContext
	if *optDatabase != "" {
		queryExecutionContext = &types.QueryExecutionContext{
			Database: optDatabase,
		}
	}
	out, err := cl.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		ClientRequestToken:       nil,
		ExecutionParameters:      nil,
		QueryExecutionContext:    queryExecutionContext,
		QueryString:              aws.String(string(b)),
		ResultConfiguration:      resultConf,
		ResultReuseConfiguration: nil,
		WorkGroup:                workGroup,
	})
	if err != nil {
		return fmt.Errorf("StartQueryExecutionWithContext: %v", err)
	}

	return getResult(ctx, cl, out, fp)
}

func marshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func getResult(ctx context.Context, cl *athena.Client, stOut *athena.StartQueryExecutionOutput, w io.Writer) error {
	var done bool
	defer func() {
		if !done {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := cl.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{QueryExecutionId: stOut.QueryExecutionId})
			if err != nil {
				log.Printf("*** StopQueryExecution: %v", err)
			} else {
				log.Printf("stopped: %s", marshalJSON(out))
			}
		}
	}()

	f, err := os.Create(*optStat)
	if err != nil {
		return fmt.Errorf("os.Create stat: %w", err)
	}
	defer f.Close()

	var stat struct {
		QueryExecutionId *string                         `json:",omitempty"`
		Statistics       *types.QueryExecutionStatistics `json:",omitempty"`
		LastStatus       *types.QueryExecutionStatus     `json:",omitempty"`
	}
	stat.QueryExecutionId = stOut.QueryExecutionId
	defer func() {
		enc := json.NewEncoder(f)
		enc.Encode(stat)
	}()

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()
	if *optTSV {
		csvWriter.Comma = '\t'
	}

	boff := backoff.NewExponentialBackOff()
	boff.MaxInterval = time.Second
	boff.MaxElapsedTime = 0
	boff.Reset()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		out, err := cl.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: stOut.QueryExecutionId,
		})
		if err != nil {
			return fmt.Errorf("GetQueryExecutionWithContext: %w", err)
		}
		stat.Statistics = out.QueryExecution.Statistics
		stat.LastStatus = out.QueryExecution.Status

		log.Printf("status=%s", marshalJSON(out.QueryExecution.Status))

		switch out.QueryExecution.Status.State {
		case types.QueryExecutionStateQueued, types.QueryExecutionStateRunning:
			d := boff.NextBackOff()
			if d == backoff.Stop {
				return fmt.Errorf("reached backoff.Stop")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
			continue
		case types.QueryExecutionStateSucceeded:
			var nextToken *string
			pc := 0
			var rec []string
			for {
				log.Printf("get result #%d", pc)
				out, err := cl.GetQueryResults(ctx, &athena.GetQueryResultsInput{
					NextToken:        nextToken,
					QueryExecutionId: stOut.QueryExecutionId,
				})
				if err != nil {
					return fmt.Errorf("GetQueryResultsWithContext: %w", err)
				}

				for _, row := range out.ResultSet.Rows {
					for _, e := range row.Data {
						if e.VarCharValue == nil {
							rec = append(rec, "")
						} else {
							rec = append(rec, *e.VarCharValue)
						}
					}

					err := csvWriter.Write(rec)
					if err != nil {
						return fmt.Errorf("csvWriter.Write: %w", err)
					}
					rec = rec[:0]
				}

				nextToken = out.NextToken
				if nextToken == nil {
					break
				}
				pc++
			}
			done = true
			return nil
		case types.QueryExecutionStateFailed:
			done = true
			return fmt.Errorf("status=%s", marshalJSON(out.QueryExecution.Status))
		default:
			return fmt.Errorf("unknown status=%s", marshalJSON(out.QueryExecution.Status))
		}
	}
}
