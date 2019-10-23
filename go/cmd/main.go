package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/influxdata/influxdb-client-go"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"strconv"
	"sync"
	"time"
)

type Writer interface {
	Write(id int, measurementName string, iteration int)
	Count(measurementName string) (int, error)
	Close() error
}

type WriterV1 struct {
	influx client.Client
}

type WriterV2 struct {
	influx influxdb.Client
}

//
// https://pragmacoders.com/blog/multithreading-in-go-a-tutorial
//
func main() {
	writerType := flag.String("type", "CLIENT_GO_V2", "Type of writer (default 'CLIENT_GO_V2'; CLIENT_GO_V1, CLIENT_GO_V2)")
	threadsCount := flag.Int("threadsCount", 2000, "how much Thread use to write into InfluxDB")
	secondsCount := flag.Int("secondsCount", 30, "how long write into InfluxDB")
	lineProtocolsCount := flag.Int("lineProtocolsCount", 100, "how much data writes in one batch")
	skipCount := flag.Bool("skipCount", false, "skip counting count")
	measurementName := flag.String("measurementName", fmt.Sprintf("sensor_%d", time.Now().UnixNano()), "writer measure destination")
	flag.Parse()

	expected := (*threadsCount) * (*secondsCount) * (*lineProtocolsCount)

	blue := color.New(color.FgHiBlue).SprintFunc()
	green := color.New(color.FgHiGreen).SprintFunc()
	fmt.Println()
	fmt.Printf("------------- %s -------------", blue(*writerType))
	fmt.Println()
	fmt.Println()
	fmt.Println("measurement:        ", *measurementName)
	fmt.Println("threadsCount:       ", *threadsCount)
	fmt.Println("secondsCount:       ", *secondsCount)
	fmt.Println("lineProtocolsCount: ", *lineProtocolsCount)
	fmt.Println()
	fmt.Println("expected size: ", expected)
	fmt.Println()

	var writer Writer
	if *writerType == "CLIENT_GO_V2" {
		influx, err := influxdb.New("http://localhost:9999", "my-token")
		if err != nil {
			panic(err)
		}
		writer = &WriterV2{
			influx: *influx,
		}
	} else {
		influx, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: "http://localhost:8086",
		})
		if err != nil {
			panic(err)
		}
		writer = &WriterV1{
			influx: influx,
		}
	}

	stopExecution := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(*threadsCount)

	start := time.Now()

	for i := 1; i <= *threadsCount; i++ {
		go doLoad(&wg, stopExecution, i, *measurementName, *secondsCount, *lineProtocolsCount, writer)
	}

	go func() {
		time.Sleep(time.Duration(*secondsCount) * time.Second)
		fmt.Printf("\n\nThe time: %v seconds elapsed! Stopping all writers\n\n", *secondsCount)
		close(stopExecution)
	}()

	wg.Wait()

	if !*skipCount {
		fmt.Println()
		fmt.Println()
		fmt.Println("Querying InfluxDB ...")
		fmt.Println()

		total, err := writer.Count(*measurementName)
		if err != nil {
			panic(err)
		}
		fmt.Println("Results:")
		fmt.Println("-> expected:        ", expected)
		fmt.Println("-> total:           ", total)
		fmt.Println("-> rate [%]:        ", (float64(total)/float64(expected))*100)
		fmt.Println("-> rate [msg/sec]:  ", green(total / *secondsCount))
		fmt.Println()
		fmt.Println("Total time:", time.Since(start))
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}
}

func doLoad(wg *sync.WaitGroup, stopExecution <-chan bool, id int, measurementName string, secondsCount int, lineProtocolsCount int, influx Writer) {
	defer wg.Done()

	for i := 1; i <= secondsCount; i++ {
		select {
		case <-stopExecution:
			return
		default:

			if id == 1 {
				fmt.Printf("\rwriting iterations: %v/%v", i, secondsCount)
			}

			start := i * lineProtocolsCount
			end := start + lineProtocolsCount
			for j := start; j < end; j++ {
				select {
				case <-stopExecution:
					return
				default:
					influx.Write(id, measurementName, j)
				}
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
}

func (p *WriterV2) Write(id int, measurementName string, iteration int) {
	record := influxdb.NewRowMetric(
		map[string]interface{}{"temperature": fmt.Sprintf("%v", time.Now().UnixNano())},
		measurementName,
		map[string]string{"id": fmt.Sprintf("%v", id)},
		time.Unix(0, int64(iteration)))

	if _, err := p.influx.Write(context.Background(), "my-bucket", "my-org", record); err != nil {
		//fmt.Println("Error: ", err)
	}
}

func (p *WriterV2) Count(measurementName string) (int, error) {
	query := "from(bucket:\"my-bucket\") " +
		"|> range(start: 0, stop: now()) " +
		"|> filter(fn: (r) => r._measurement == \"" + measurementName + "\") " +
		"|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
		"|> drop(columns: [\"id\"]) " +
		"|> count(column: \"temperature\")"

	queryResult, err := p.influx.QueryCSV(context.Background(), query, "my-org")
	if err != nil {
		return 0, err
	}
	records, err := csv.NewReader(bufio.NewReader(queryResult)).ReadAll()
	if err != nil {
		return 0, err
	}
	total, err := strconv.Atoi(records[4][6])
	if err != nil {
		return 0, err
	}

	return total, nil
}
func (p *WriterV2) Close() error {
	return p.influx.Close()
}

func (p *WriterV1) Write(id int, measurementName string, iteration int) {

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: "iot_writes",
	})

	tags := map[string]string{"id": fmt.Sprintf("%v", id)}
	fields := map[string]interface{}{
		"temperature": fmt.Sprintf("%v", time.Now().UnixNano()),
	}
	pt, _ := client.NewPoint(measurementName, tags, fields, time.Unix(0, int64(iteration)))
	bp.AddPoint(pt)
	if err := p.influx.Write(bp); err != nil {

	}
}
func (p *WriterV1) Count(measurementName string) (int, error) {
	q := client.NewQuery("SELECT count(*) FROM "+measurementName, "iot_writes", "")
	if response, err := p.influx.Query(q); err == nil && response.Error() == nil {
		count := response.Results[0].Series[0].Values[0][1]
		i, err := strconv.Atoi(fmt.Sprintf("%v", count))
		return i, err
	}
	return 0, nil
}
func (p *WriterV1) Close() error { return p.influx.Close() }
