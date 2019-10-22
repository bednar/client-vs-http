package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/influxdata/influxdb-client-go"
	"strconv"
	"sync"
	"time"
)

//
// https://pragmacoders.com/blog/multithreading-in-go-a-tutorial
//
func main() {
	influx, err := influxdb.New("http://localhost:9999", "my-token")
	if err != nil {
		panic(err)
	}
	threadsCount := flag.Int("threadsCount", 2000, "how much Thread use to write into InfluxDB")
	secondsCount := flag.Int("secondsCount", 30, "how long write into InfluxDB")
	lineProtocolsCount := flag.Int("lineProtocolsCount", 100, "how much data writes in one batch")
	skipCount := flag.Bool("skipCount", false, "skip counting count")
	measurementName := flag.String("measurementName", fmt.Sprintf("sensor_%d", time.Now().UnixNano()), "a measure destination")
	flag.Parse()

	expected := (*threadsCount) * (*secondsCount) * (*lineProtocolsCount)

	blue := color.New(color.FgHiBlue).SprintFunc()
	green := color.New(color.FgHiGreen).SprintFunc()
	fmt.Println()
	fmt.Printf("------------- %s -------------", blue("CLIENT_GO_V2"))
	fmt.Println()
	fmt.Println()
	fmt.Println("measurement:        ", *measurementName)
	fmt.Println("threadsCount:       ", *threadsCount)
	fmt.Println("secondsCount:       ", *secondsCount)
	fmt.Println("lineProtocolsCount: ", *lineProtocolsCount)
	fmt.Println()
	fmt.Println("expected size: ", expected)
	fmt.Println()

	stopExecution := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(*threadsCount)

	start := time.Now()

	for i := 1; i <= *threadsCount; i++ {
		go doLoad(&wg, stopExecution, i, *measurementName, *secondsCount, *lineProtocolsCount, *influx)
	}

	go func() {
		time.Sleep(time.Duration(*secondsCount) * time.Second)
		fmt.Printf("\n\nThe time: %v seconds elapsed! Stopping all writers\n", *secondsCount)
		close(stopExecution)
	}()

	wg.Wait()

	if !*skipCount {
		fmt.Println()
		fmt.Println()
		fmt.Println("Querying InfluxDB 2.0...")
		fmt.Println()
		query := "from(bucket:\"my-bucket\") " +
			"|> range(start: 0, stop: now()) " +
			"|> filter(fn: (r) => r._measurement == \"" + *measurementName + "\") " +
			"|> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\") " +
			"|> drop(columns: [\"id\"]) " +
			"|> count(column: \"temperature\")"
		queryResult, err := influx.QueryCSV(context.Background(), query, "my-org")
		if err != nil {
			panic(err)
		}
		records, err := csv.NewReader(bufio.NewReader(queryResult)).ReadAll()
		if err != nil {
			panic(err)
		}
		total, err := strconv.Atoi(records[4][6])
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

	err = influx.Close()
	if err != nil {
		panic(err)
	}
}

func doLoad(wg *sync.WaitGroup, stopExecution <-chan bool, id int, measurementName string, secondsCount int, lineProtocolsCount int, influx influxdb.Client) {
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
					record := influxdb.NewRowMetric(
						map[string]interface{}{"temperature": fmt.Sprintf("%v", time.Now().UnixNano())},
						measurementName,
						map[string]string{"id": fmt.Sprintf("%v", id)},
						time.Unix(0, int64(j)))

					if _, err := influx.Write(context.Background(), "my-bucket", "my-org", record); err != nil {
						//fmt.Println("Error: ", err)
					}
				}
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
}
