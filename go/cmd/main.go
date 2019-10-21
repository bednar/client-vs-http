package main

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"sync"
	"time"
)

//
// https://pragmacoders.com/blog/multithreading-in-go-a-tutorial
//
func main() {
	threadsCount := flag.Int("threadsCount", 2000, "how much Thread use to write into InfluxDB")
	secondsCount := flag.Int("secondsCount", 30, "how long write into InfluxDB")
	lineProtocolsCount := flag.Int("lineProtocolsCount", 100, "how much data writes in one batch")
	measurementName := fmt.Sprintf("sensor_%d", time.Now().UnixNano())

	blue := color.New(color.FgHiBlue).SprintFunc()
	green := color.New(color.FgHiGreen).SprintFunc()
	fmt.Println()
	fmt.Printf("------------- %s -------------", blue("CLIENT_GO"))
	fmt.Println()
	fmt.Println()
	fmt.Println("measurement:        ", measurementName)
	fmt.Println("threadsCount:       ", *threadsCount)
	fmt.Println("secondsCount:       ", *secondsCount)
	fmt.Println("lineProtocolsCount: ", *lineProtocolsCount)
	fmt.Println()
	fmt.Println("expected size: ", (*threadsCount)*(*secondsCount)*(*lineProtocolsCount))
	fmt.Println()

	var wg sync.WaitGroup
	wg.Add(*threadsCount)

	start := time.Now()

	for i := 1; i <= *threadsCount; i++ {
		go doLoad(&wg, i, measurementName, *secondsCount, *lineProtocolsCount)
	}

	wg.Wait()

	fmt.Println()
	fmt.Println()
	fmt.Println("Querying InfluxDB 2.0...")
	fmt.Println()

	fmt.Println("Results:")
	fmt.Println("-> expected:        ", (*threadsCount)*(*secondsCount)*(*lineProtocolsCount))
	fmt.Println("-> total:           ", "???")
	fmt.Println("-> rate [%]:        ", "???")
	fmt.Println("-> rate [msg/sec]:  ", green("???"))
	fmt.Println()
	fmt.Println("Total time:", time.Since(start))
}

func doLoad(wg *sync.WaitGroup, id int, measurementName string, secondsCount int, lineProtocolsCount int) {
	defer wg.Done()

	for i := 1; i <= secondsCount; i++ {
		if id == 1 {
			fmt.Printf("\rwriting iterations: %v/%v", i, secondsCount)
		}

		start := i * lineProtocolsCount
		end := start + lineProtocolsCount
		for j := start; j < end; j++ {
			_ = fmt.Sprintf("%s,id=%v temperature=%v %v", measurementName, id, time.Now().UnixNano(), j)
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
}
