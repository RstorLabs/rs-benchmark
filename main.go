/*
# rs-benchmark - A utility to benchmark object storages
# Copyright (C) 2016-2019 RStor Inc (open-source@rstor.io)
#
# This file is part of rs-benchmark.
#
# rs-benchmark is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# rs-benchmark is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Copyright Header.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/bytefmt"
	log "github.com/sirupsen/logrus"
)

var (
	objectSize                   uint64
	partSize                     uint64
	objectData                   []byte
	verbose                      bool
	durationSecs, threads, loops int
	successFulUploadsIDs         []int
	multipartConcurrency         int
	objPrefix                    string
	maxRetries                   int
	noCleanup                    bool
	noGet                        bool
	client                       Uploader
)

const version = "1.0"

func main() {
	var accessKey, secretKey, urlHost, bucket, region, sizeArg, multipartSizeArg string
	var protocol string
	var useMultipart, showVersion bool
	var pauseBetweenPhases bool
	var hostIP string

	// Parse command line
	fl := flag.NewFlagSet("rs-benchmark", flag.ExitOnError)
	fl.StringVar(&accessKey, "a", "", "Access key")
	fl.StringVar(&secretKey, "s", "", "Secret key")
	fl.StringVar(&urlHost, "u", "", "URL for endpoint with method prefix (e.g. https://s3.YOUR_CUSTOMER_NAME.rstorlabs.io)")
	fl.StringVar(&bucket, "b", "", "Bucket for testing")
	fl.IntVar(&durationSecs, "d", 60, "Duration of each test in seconds")
	fl.IntVar(&threads, "t", 1, "Number of parallel requests to run")
	fl.IntVar(&loops, "l", 1, "Number of times to repeat test")
	fl.BoolVar(&verbose, "v", false, "Verbose error output")
	fl.BoolVar(&showVersion, "version", false, "Show version")
	fl.StringVar(&region, "r", "", "Region for testing")
	fl.StringVar(&protocol, "protocol", "", "client protocol: s3v2, s3v4, azure, gcp")
	fl.BoolVar(&useMultipart, "multipart", false, "use multipart")
	fl.IntVar(&multipartConcurrency, "multipart-concurrency", 5, "concurrency to use for multipart requests")
	fl.BoolVar(&pauseBetweenPhases, "pause", false, "whether to pause between upload and download tests")
	fl.BoolVar(&noCleanup, "no-cleanup", false, "do not cleanup inserted data")
	fl.BoolVar(&noGet, "no-get", false, "do not perform GET part of test")
	fl.StringVar(&hostIP, "ip", "", "forces all hostnames to resolve to this address (s3v2, s3v4 only)")
	fl.StringVar(&objPrefix, "prefix", "Object", "will create objects with key: 'prefix-number'")
	fl.IntVar(&maxRetries, "maxRetries", 0, "number of retries on failure (default 0. s3v4 only)")
	fl.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with suffix K, M, and G")
	fl.StringVar(&multipartSizeArg, "multipart-size", "5M", "Size of the multipart chunks")

	switch err := fl.Parse(os.Args[1:]); err {
	case flag.ErrHelp:
		fmt.Println("Available arguments:")
		fl.PrintDefaults()
		fmt.Println("")
		os.Exit(0)
	case nil:
	default:
		fmt.Printf("Unable to parse flags %v\n", err)
	}

	// Check the arguments
	if showVersion == true {
		fmt.Printf("RStor rs-benchmark v%s.\n\n", version)
		os.Exit(0)
	}

	fmt.Printf("rs-benchmark v%s - a compact tool for benchmarking different object storages\n", version)
	fmt.Println("Copyright (C) 2016-2020 RStor Inc (open-source@rstor.io)")
	fmt.Println("Released under GPL v3 license")
	fmt.Println()

	if protocol == "" {
		fmt.Println("Missing argument -protocol for client protocol.")
		printHelpAndExit()
	}

	if protocol == "s3v4" && region == "" {
		fmt.Println("Protocol s3v4 requires the region to be specified.")
		printHelpAndExit()
		os.Exit(-1)
	}

	hostIPForPrinting := ""
	if hostIP == "" && urlHost == "" {
		fmt.Println("Missing host information.")
		printHelpAndExit()
	}

	if hostIP != "" {
		dTransport := httpClient.Transport.(*http.Transport)

		dTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			addr = hostIP
			return dialer.DialContext(ctx, network, addr)
		}

		hostIPForPrinting = hostIP
	} else {
		u, err := url.Parse(urlHost)
		if err != nil {
			fmt.Println("Invalid url ", err)
			printHelpAndExit()
		}

		host := strings.Split(u.Host, ":")[0]
		ips, err := net.LookupIP(host)
		if err != nil {
			fmt.Println("Can't resolve host ", u.Host)
			printHelpAndExit()
		} else {
			var ipStrings []string
			for _, ip := range ips {
				ipStrings = append(ipStrings, ip.String())
			}
			hostIPForPrinting = strings.Join(ipStrings, ", ")
		}
	}

	if protocol != "gcp" {
		if accessKey == "" {
			fmt.Println("Missing argument -a for access key.")
			printHelpAndExit()
		}
		if secretKey == "" {
			fmt.Println("Missing argument -s for secret key.")
			printHelpAndExit()
		}
	}

	if bucket == "" {
		fmt.Println("Missing argument -b for bucket.")
		printHelpAndExit()
	}

	var err error

	if objectSize, err = bytefmt.ToBytes(sizeArg); err != nil {
		fmt.Printf("Invalid -z argument for object size: %v\n", err)
		printHelpAndExit()
	}

	if partSize, err = bytefmt.ToBytes(multipartSizeArg); err != nil {
		fmt.Printf("Invalid -multipart-size argument for part size: %v\n", err)
		printHelpAndExit()
	}

	switch protocol {
	case "s3v4":
		v4Client := NewS3AwsV4(accessKey, secretKey, urlHost, region)
		v4Client.UseMultipart = useMultipart
		client = v4Client
	case "s3v2":
		if useMultipart {
			fmt.Println("Multipart not supported")
			printHelpAndExit()
		}
		if region != "" {
			fmt.Println("-region not supported for s3v2. Drop option.")
			printHelpAndExit()
		}
		client = NewS3AwsV2(accessKey, secretKey, urlHost, region)
	case "azure":
		if region != "" {
			fmt.Println("region param not supported yet")
		}
		if useMultipart && multipartConcurrency > 1 {
			fmt.Println("Multipart concurrency is fixed to one")
			multipartConcurrency = 1
		}
		aup := NewAzureUploader(accessKey, secretKey, urlHost, region)
		aup.UseMultipart = useMultipart
		client = aup
	case "gcp":
		if region != "" {
			fmt.Println("region param not supported yet")
		}
		if useMultipart && multipartConcurrency > 1 {
			fmt.Println("Multipart concurrency is fixed to one")
			multipartConcurrency = 1
		}
		gup := NewGCP(accessKey, secretKey, urlHost, region)
		gup.UseMultipart = useMultipart
		client = gup
	default:
		fmt.Println("unknown client type: available: s3v4, s3v2, azure, gpc")
		printHelpAndExit()
	}
	fmt.Println("Benchmark parameters:")

	fmt.Printf("%-15s%s\n", "Endpoint URL", urlHost)
	fmt.Printf("%-15s%s\n", "Protocol", protocol)
	fmt.Printf("%-15s%s\n", "Host ip", hostIPForPrinting)
	fmt.Printf("%-15s%s\n", "Bucket", bucket)
	if region != "" {
		fmt.Printf("%-15s%s\n", "Region", region)
	}
	fmt.Printf("%-15s%d\n", "Test time", durationSecs)
	fmt.Printf("%-15s%d\n", "Threads", threads)
	fmt.Printf("%-15s%s\n", "Size", sizeArg)
	fmt.Printf("%-15s%d\n", "Loops", loops)
	fmt.Printf("%-15s%t", "Multipart", useMultipart)
	if useMultipart == true {
		fmt.Printf(", %s per part, %d parallel uploads", multipartSizeArg, multipartConcurrency)
	}
	fmt.Println("")
	fmt.Printf("%-15s%d\n", "Max retries", maxRetries)

	// Test access to the bucket
	err = client.Prepare(bucket)
	if err != nil {
		log.Fatal(err)
		fmt.Println("For more information, run again with flag -v.")
	}

	// Initialize data for the bucket
	objectData = make([]byte, objectSize)
	rand.Read(objectData)

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {
		runLoop(loop, pauseBetweenPhases)
	}

	fmt.Println("\nDone.")
}

func runLoop(loop int, pauseBetweenPhases bool) {
	if loop > 1 && pauseBetweenPhases {
		fmt.Printf("Loop %d done\n", loop-1)
		pause()
	}

	fmt.Printf("\nStarting loop %d...\n", loop)

	// Run the upload case
	successFulUploadsIDs = make([]int, 0, 1000) // reused in download step
	totalDuration := float64(0)
	indexes := make(chan int, threads)
	res := make(chan TransferResult, threads)

	ctx, cancelRemainingUploads := context.WithCancel(context.Background())
	for n := 0; n <= threads; n++ {
		go runUpload(ctx, indexes, res)
	}

	startTime := time.Now()
	uploads := runAndCollectResults(indexes, res)
	cancelRemainingUploads()
	uploadTime := time.Now().Sub(startTime).Seconds()

	var uploadedBytes uint64
	var uploadDurations []float64
	for _, v := range uploads {
		if v.Error != nil {
			continue
		}
		successFulUploadsIDs = append(successFulUploadsIDs, v.Id)
		uploadedBytes += objectSize
		uploadDurations = append(uploadDurations, v.Duration.Seconds())
		totalDuration += v.Duration.Seconds()
	}
	sort.Float64s(uploadDurations)

	// log.Info(successFulUploadsIDs)
	successfulUploads := len(successFulUploadsIDs)
	failedUploads := len(uploads) - len(successFulUploadsIDs)
	uploadMBps := (float64(uploadedBytes) / uploadTime) / (1000 * 1000)
	uploadPerSecond := float64(successfulUploads) / uploadTime

	fmt.Printf("%-9s%-6s%-11s%-7s%-12s%-8s%-8s%-12s\n",
		"Threads", "Size", "Operation", "Time", "Successful", "Failed", "MBps", "OPps")
	fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-8.2f%-12.2f\n",
		threads, bytefmt.ByteSize(objectSize), "PUT", uploadTime, successfulUploads, failedUploads, uploadMBps, uploadPerSecond)

	if len(successFulUploadsIDs) < 5 {
		if verbose == false {
			log.Fatal("Not enough successful uploads to continue. For more information, run again with flag -v")
		} else {
			log.Fatal("Not enough successful uploads to continue.")
		}
	}

	if pauseBetweenPhases {
		pause()
	}

	// Run the download case
	indexes = make(chan int, threads)
	res = make(chan TransferResult, 10)

	ctx, cancelRemainingDownloads := context.WithCancel(context.Background())
	for n := 0; n <= threads; n++ {
		go runDownload(ctx, indexes, res)
	}

	startTime = time.Now()
	downloads := runAndCollectResults(indexes, res)
	cancelRemainingDownloads()
	downloadTime := time.Now().Sub(startTime).Seconds()

	var successfulDownloads int
	var failedDownloads int
	var downloadedBytes uint64
	totalDuration = 0
	var downloadDurations []float64
	for _, d := range downloads {
		if d.Error != nil {
			failedDownloads++
		} else {
			successfulDownloads++
			downloadedBytes += objectSize
			totalDuration += d.Duration.Seconds()
			downloadDurations = append(downloadDurations, d.Duration.Seconds())
		}
	}
	sort.Float64s(downloadDurations)

	if successfulDownloads == 0 {
		log.Fatal("All downloads failed")
		fmt.Println("For more information, run again with flag -v.")
	}

	mbPs := (float64(downloadedBytes) / downloadTime) / (1000 * 1000)
	getPerSecond := float64(successfulDownloads) / downloadTime

	fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-8.2f%-12.2f\n",
		threads, bytefmt.ByteSize(objectSize), "GET", downloadTime, successfulDownloads, failedDownloads, mbPs, getPerSecond)

	if noCleanup {
		fmt.Println("Not performing cleanup, as requested")
		return
	}

	if pauseBetweenPhases {
		pause()
	}

	ctx = context.Background()
	fmt.Println("Deleting test objects")
	deleteChan := make(chan int)
	deleteWg := sync.WaitGroup{}
	deleteWg.Add(threads)
	for n := 0; n <= threads; n++ {
		go func() {
			defer deleteWg.Done()

			for idx := range deleteChan {
				_ = client.DoDelete(ctx, idx)
			}
		}()
	}
	for i, v := range successFulUploadsIDs {
		deleteChan <- v
		if i > 0 && i%1000 == 0 {
			fmt.Printf("%d deletes completed\n", i)
		}
	}
	close(deleteChan)
	deleteWg.Wait()
}

func runAndCollectResults(indexes chan int, res chan TransferResult) []TransferResult {
	var nextId int
	for nextId = 0; nextId < threads+1; nextId++ {
		indexes <- nextId
	}

	results := make([]TransferResult, 0, 1000)
	deadline := time.After(time.Second * time.Duration(durationSecs))

Loop:
	for {
		select {
		case <-deadline:
			break Loop
		case r := <-res:
			results = append(results, r)
			if r.Error != nil {
				indexes <- r.Id
			} else {
				indexes <- nextId
				nextId = nextId + 1
			}
		}
	}

	return results
}

type TransferResult struct {
	Id       int
	Duration time.Duration
	Error    error
}

func runUpload(ctx context.Context, ids chan int, res chan TransferResult) {
	for id := range ids {
		reader := bytes.NewReader(objectData)

		startTime := time.Now()
		r := client.DoUpload(ctx, id, reader)

		r.Duration = time.Now().Sub(startTime)
		r.Id = id

		if r.Error != nil && verbose {
			if !strings.Contains(r.Error.Error(), "context canceled") {
				log.Error(r.Error)
			}
		}
		res <- r
	}
}

func runDownload(ctx context.Context, indexes chan int, res chan TransferResult) {
	for id := range indexes {
		idx := successFulUploadsIDs[id%len(successFulUploadsIDs)]

		startTime := time.Now()
		r := client.DoDownload(ctx, idx)

		r.Duration = time.Now().Sub(startTime)
		r.Id = id

		if r.Error != nil && verbose {
			if !strings.Contains(r.Error.Error(), "context canceled") {
				log.Error(r.Error)
			}
		}
		res <- r
	}
}

func pause() {
	fmt.Print("Press 'Enter' to continue to the next phase")
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func printHelpAndExit() {
	fmt.Print("Abort.\n\nRun \"./rs-benchmark --help\" for usage.\n")
	os.Exit(-1)
}
