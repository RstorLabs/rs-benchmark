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
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	log "github.com/sirupsen/logrus"
)

var object_size uint64
var part_size uint64
var object_data []byte
var verbose bool
var duration_secs, threads, loops int
var client Uploader
var successFulUploadsIDs []int
var multipartConcurrency int
var objPrefix string
var maxRetries int
var version string

func main() {

	version = "1.0"

	var access_key, secret_key, url_host, bucket, region, sizeArg, multipartSizeArg string
	var protocol string
	var useMultipart, help, showVersion bool
	var pauseBetweenPhases bool
	var hostIP string

	// Parse command line
	myflag := flag.NewFlagSet("rs-benchmark", flag.ExitOnError)
	myflag.StringVar(&access_key, "a", "", "Access key")
	myflag.StringVar(&secret_key, "s", "", "Secret key")
	myflag.StringVar(&url_host, "u", "", "URL for endpoint with method prefix (e.g. https://s3.YOUR_CUSTOMER_NAME.rstorlabs.io)")
	myflag.StringVar(&bucket, "b", "", "Bucket for testing")
	myflag.BoolVar(&help, "h", false, "Show help screen")
	myflag.IntVar(&duration_secs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of parallel requests to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	myflag.BoolVar(&verbose, "v", false, "Verbose error output")
	myflag.BoolVar(&showVersion, "version", false, "Show version")
	myflag.StringVar(&region, "r", "", "Region for testing")
	myflag.StringVar(&protocol, "protocol", "", "client protocol: s3v2, s3v4, azure, gcp")
	myflag.BoolVar(&useMultipart, "multipart", false, "use multipart")
	myflag.IntVar(&multipartConcurrency, "multipart-concurrency", 5, "concurrency to use for multipart requests")
	myflag.BoolVar(&pauseBetweenPhases, "pause", false, "whether to pause between upload and download tests")
	myflag.StringVar(&hostIP, "ip", "", "forces all hostnames to resolve to this address (s3v2, s3v4 only)")
	myflag.StringVar(&objPrefix, "prefix", "Object", "will create objects with key: 'prefix-number'")
	myflag.IntVar(&maxRetries, "maxRetries", 0, "number of retries on failure (default 0. s3v4 only)")
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with suffix K, M, and G")
	myflag.StringVar(&multipartSizeArg, "multipart-size", "5M", "Size of the multipart chunks")

	//If no arguments are passed
	if len(os.Args) == 1 {
		fmt.Printf("usage: ./rs-benchmark [OPTIONS]\n\n")
		fmt.Println("For help, run ./rs-benchmark -h.")
		os.Exit(-1)
	}

	//for --help flag - need to find a more elegant solution
	if os.Args[1] == "--help" {
		fmt.Println("Available arguments:")
		myflag.PrintDefaults()
		fmt.Println("")
		os.Exit(0)
	}

	// Parse arguments
	if err := myflag.Parse(os.Args[1:]); err != nil {
		fmt.Println("Unable to parse flags")
		printHelp()
	}

	// Check the arguments

	if showVersion == true {
		fmt.Printf("RStor rs-benchmark v%s.\n\n", version)
		os.Exit(0)
	}

	fmt.Printf("rs-benchmark v%s - a compact tool for benchmarking different object storages\n", version)
	fmt.Println("Copyright (C) 2016-2019 RStor Inc (open-source@rstor.io)")
	fmt.Println("Released under GPL v3 license")

	if help == true {
		fmt.Println("Available arguments:")
		myflag.PrintDefaults()
		fmt.Println("")
		os.Exit(0)
	}

	if protocol == "" {
		fmt.Println("Missing argument -protocol for client protocol.")
		printHelp()

	}

	if protocol == "s3v4" && region == "" {
		fmt.Println("Protocol s3v4 requires the region to be specified.")
		printHelp()
		os.Exit(-1)
	}

	hostIPForPrinting := ""
	if hostIP == "" && url_host == "" {
		fmt.Println("Missing host information.")
		printHelp()
	}

	if hostIP != "" {
		dTransport := httpClient.Transport.(*http.Transport)

		dTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			addr = hostIP
			return dialer.DialContext(ctx, network, addr)
		}

		hostIPForPrinting = hostIP
	} else {
		u, err := url.Parse(url_host)
		if err != nil {
			fmt.Println("Invalid url ", err)
			printHelp()
		}

		host := strings.Split(u.Host, ":")[0]
		ips, err := net.LookupIP(host)
		if err != nil {
			fmt.Println("Can't resolve host ", u.Host)
			printHelp()
		} else {
			var ipStrings []string
			for _, ip := range ips {
				ipStrings = append(ipStrings, ip.String())
			}
			hostIPForPrinting = strings.Join(ipStrings, ", ")
		}
	}

	if protocol != "gcp" {
		if access_key == "" {
			fmt.Println("Missing argument -a for access key.")
			printHelp()
		}
		if secret_key == "" {
			fmt.Println("Missing argument -s for secret key.")
			printHelp()
		}
	}

	if bucket == "" {
		fmt.Println("Missing argument -b for bucket.")
		printHelp()
	}

	var err error

	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		fmt.Printf("Invalid -z argument for object size: %v\n", err)
		printHelp()
	}

	if part_size, err = bytefmt.ToBytes(multipartSizeArg); err != nil {
		fmt.Printf("Invalid -multipart-size argument for part size: %v\n", err)
		printHelp()
	}

	switch protocol {
	case "s3v4":
		v4Client := NewS3AwsV4(access_key, secret_key, url_host, region)
		v4Client.UseMultipart = useMultipart
		client = v4Client
	case "s3v2":
		if useMultipart {
			fmt.Println("Multipart not supported")
			printHelp()
		}
		if region != "" {
			fmt.Println("-region not supported for s3v2. Drop option.")
			printHelp()
		}
		client = NewS3AwsV2(access_key, secret_key, url_host, region)
	case "azure":
		if region != "" {
			fmt.Println("region param not supported yet")
		}
		if useMultipart && multipartConcurrency > 1 {
			fmt.Println("Multipart concurrency is fixed to one")
			multipartConcurrency = 1
		}
		aup := NewAzureUploader(access_key, secret_key, url_host, region)
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
		gup := NewGCP(access_key, secret_key, url_host, region)
		gup.UseMultipart = useMultipart
		client = gup
	default:
		fmt.Println("unknown client type: available: s3v4, s3v2, azure, gpc")
		printHelp()
	}
	fmt.Println("Benchmark parameters:")

	fmt.Printf("%-15s%s\n", "Endpoint URL", url_host)
	fmt.Printf("%-15s%s\n", "Protocol", protocol)
	fmt.Printf("%-15s%s\n", "Host ip", hostIPForPrinting)
	fmt.Printf("%-15s%s\n", "Bucket", bucket)
	if region != "" {
		fmt.Printf("%-15s%s\n", "Region", region)
	}
	fmt.Printf("%-15s%d\n", "Test time", duration_secs)
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

	indexes := make(chan int, threads)
	res := make(chan TransferResult, threads)
	successFulUploadsIDs = make([]int, 0, 1000)

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
	var totalDuration float64
	for _, v := range uploads {
		if v.Error != nil {
			continue
		}
		successFulUploadsIDs = append(successFulUploadsIDs, v.Id)
		uploadedBytes += object_size
		uploadDurations = append(uploadDurations, v.Duration.Seconds())
		totalDuration += v.Duration.Seconds()
	}
	sort.Float64s(uploadDurations)

	// log.Info(successFulUploadsIDs)
	successfulUploads := len(successFulUploadsIDs)
	failedUploads := len(uploads) - len(successFulUploadsIDs)
	uploadMBps := (float64(uploadedBytes) / uploadTime) / (1000 * 1000)

	fmt.Printf("%-9s%-6s%-11s%-7s%-12s%-8s%-6s\n", "Threads", "Size", "Operation", "Time", "Successful", "Failed", "MBps")
	fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-6.2f\n",
		threads, bytefmt.ByteSize(object_size), "PUT", uploadTime, successfulUploads, failedUploads, uploadMBps)

	if len(successFulUploadsIDs) < 5 {
		log.Fatal("Not enough successful uploads to continue.")
		if verbose == false {
			fmt.Println("For more information, run again with flag -v.")
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
			downloadedBytes += object_size
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

	fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-6.2f\n",
		threads, bytefmt.ByteSize(object_size), "GET", downloadTime, successfulDownloads, failedDownloads, mbPs)

	if pauseBetweenPhases {
		pause()
	}

	ctx = context.Background()
	fmt.Println("Deleting test objects")
	for i, v := range successFulUploadsIDs {
		if i > 0 && i%1000 == 0 {
			fmt.Printf("%d deletes completed\n", i)
		}
		_ = client.DoDelete(ctx, v)
	}
}

func runAndCollectResults(indexes chan int, res chan TransferResult) []TransferResult {
	var nextId int
	for nextId = 0; nextId < threads+1; nextId++ {
		indexes <- nextId
	}

	results := make([]TransferResult, 0, 1000)
	deadline := time.After(time.Second * time.Duration(duration_secs))

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
		reader := InfiniteReader{Size: int64(object_size)}

		startTime := time.Now()
		r := client.DoUpload(ctx, id, &reader)

		r.Duration = time.Now().Sub(startTime)
		r.Id = id

		if r.Error != nil && verbose {
			if strings.Contains(r.Error.Error(), "context canceled") {
				log.Info(r.Error)
			} else {
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
			if strings.Contains(r.Error.Error(), "context canceled") {
				log.Info(r.Error)
			} else {
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

func printHelp() {
	fmt.Print("Abort.\n\nRun \"./rs-benchmark --help\" for usage.\n")
	os.Exit(-1)
}
