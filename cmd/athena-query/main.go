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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
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

	sess := session.Must(session.NewSessionWithOptions(
		session.Options{SharedConfigState: session.SharedConfigEnable}))

	cl := athena.New(sess)

	var resultConf *athena.ResultConfiguration
	if *optOutputLocation != "" {
		resultConf = &athena.ResultConfiguration{
			OutputLocation: aws.String(*optOutputLocation),
		}
	}

	var workGroup *string
	if *optWorkGroup != "" {
		workGroup = optWorkGroup
	}

	var queryExecutionContext *athena.QueryExecutionContext
	if *optDatabase != "" {
		queryExecutionContext = &athena.QueryExecutionContext{
			Database: optDatabase,
		}
	}
	out, err := cl.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
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

func getResult(ctx context.Context, cl *athena.Athena, stOut *athena.StartQueryExecutionOutput, w io.Writer) error {
	var done bool
	defer func() {
		if !done {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cl.StopQueryExecutionWithContext(ctx, &athena.StopQueryExecutionInput{QueryExecutionId: stOut.QueryExecutionId})
		}
	}()

	f, err := os.Create(*optStat)
	if err != nil {
		return fmt.Errorf("os.Create stat: %w", err)
	}
	defer f.Close()

	var stat struct {
		QueryExecutionId *string                          `json:",omitempty"`
		Statistics       *athena.QueryExecutionStatistics `json:",omitempty"`
		LastStatus       *athena.QueryExecutionStatus     `json:",omitempty"`
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		out, err := cl.GetQueryExecutionWithContext(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: stOut.QueryExecutionId,
		})
		if err != nil {
			return fmt.Errorf("GetQueryExecutionWithContext: %w", err)
		}
		stat.Statistics = out.QueryExecution.Statistics
		stat.LastStatus = out.QueryExecution.Status

		log.Printf("status=%s", out.QueryExecution.Status)

		switch *out.QueryExecution.Status.State {
		case "QUEUED", "RUNNING":
			if err := func() error {
				tm := time.NewTimer(1 * time.Second)
				defer tm.Stop()
				select {
				case <-tm.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}(); err != nil {
				return err
			}
			continue
		case "SUCCEEDED":
			var nextToken *string
			pc := 0
			var rec []string
			for {
				log.Printf("get result #%d", pc)
				out, err := cl.GetQueryResultsWithContext(ctx, &athena.GetQueryResultsInput{
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
		case "FAILED":
			done = true
			return fmt.Errorf("status=%s", out.QueryExecution.Status)
		default:
			return fmt.Errorf("unknown status=%s", out.QueryExecution.Status)
		}
	}
}
