package main

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"time"
)

func main() {
	threadsCount := flag.Int("threadsCount", 2000, "how much Thread use to write into InfluxDB")
	secondsCount := flag.Int("secondsCount", 30, "how long write into InfluxDB")
	lineProtocolsCount := flag.Int("lineProtocolsCount", 100, "how much data writes in one batch")
	measurement := fmt.Sprintf("sensor_%d", time.Now().UnixNano())

	blue := color.New(color.FgBlue).SprintFunc()
	fmt.Println()
	fmt.Printf("------------- %s -------------", blue("CLIENT_GO"))
	fmt.Println()
	fmt.Println()
	fmt.Println("measurement:        ", measurement)
	fmt.Println("threadsCount:       ", *threadsCount)
	fmt.Println("secondsCount:       ", *secondsCount)
	fmt.Println("lineProtocolsCount: ", *lineProtocolsCount)
	fmt.Println()
	fmt.Println("expected size: ", (*threadsCount)*(*secondsCount)*(*lineProtocolsCount))
}
