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
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	objectSize                   uint64
	partSize                     uint64
	objectData                   []byte
	verbose                      bool
	durationSecs, threads, loops int
	successfulUploadIDs          []int
	multipartConcurrency         int
	objPrefix                    string
	distributeKeys               bool
	maxRetries                   int
	noCleanup                    bool
	noGet                        bool
	objects                      uint64
	client                       Uploader
	detailed                     bool
)

const version = "1.1"

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
	fl.StringVar(&urlHost, "u", "", "URL for endpoint with method prefix (e.g. https://s3.rstorcloud.io)")
	fl.StringVar(&bucket, "b", "", "Bucket for testing")
	fl.IntVar(&durationSecs, "d", 60, "Duration of each test in seconds")
	fl.Uint64Var(&objects, "objects", 0, "Number of objects to successfully upload/download")
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
	fl.StringVar(&objPrefix, "prefix", "", "will create objects with key: 'prefix/number', defaults to rsbench-$unixtimestamp")
	fl.BoolVar(&distributeKeys, "distribute-keys", true, "distribute keys over two levels of directories (65536 prefixes)")
	fl.IntVar(&maxRetries, "max-retries", 0, "number of retries on failure (default 0. s3v4 only)")
	fl.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with suffix K, M, and G")
	fl.StringVar(&multipartSizeArg, "multipart-size", "5M", "Size of the multipart chunks")
	fl.BoolVar(&detailed, "detailed", false, "print detailed stats")

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
		fmt.Printf("RSTOR rs-benchmark v%s.\n\n", version)
		os.Exit(0)
	}

	fmt.Printf("rs-benchmark v%s - a compact tool for benchmarking different object storages\n", version)
	fmt.Println("Copyright (C) 2016-2020 RStor Inc (open-source@rstor.io)")
	fmt.Println("Released under GPL v3 license")
	fmt.Println()

	if objPrefix == "" {
		objPrefix = fmt.Sprintf("rsbench-%d", time.Now().Unix())
	}

	if protocol == "" {
		fmt.Println("Missing argument -protocol for client protocol.")
		printHelpAndExit()
	}

	if protocol == "s3v4" && region == "" {
		fmt.Println("Protocol s3v4 requires the region to be specified.")
		printHelpAndExit()
		os.Exit(-1)
	}

	if hostIP == "" && urlHost == "" {
		fmt.Println("Missing host information.")
		printHelpAndExit()
	}

	hostIPForPrinting := ""
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

	if multipartConcurrency <= 0 {
		multipartConcurrency = 1
	}

	if maxRetries < 0 {
		maxRetries = 0
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
		client = NewS3AwsV2(accessKey, secretKey, urlHost)
	case "azure":
		if region != "" {
			fmt.Println("region param not supported")
			printHelpAndExit()
		}
		if useMultipart && multipartConcurrency > 1 {
			fmt.Println("Multipart concurrency is fixed to one")
			multipartConcurrency = 1
		}
		aup := NewAzureUploader(accessKey, secretKey, urlHost)
		aup.UseMultipart = useMultipart
		client = aup
	case "gcp":
		if region != "" {
			fmt.Println("region param not supported")
			printHelpAndExit()
		}
		if useMultipart && multipartConcurrency > 1 {
			fmt.Println("Multipart concurrency is fixed to one")
			multipartConcurrency = 1
		}
		gup := NewGCP()
		gup.UseMultipart = useMultipart
		client = gup
	default:
		fmt.Println("unknown client type. Available: s3v4, s3v2, azure, gpc")
		printHelpAndExit()
	}
	fmt.Println("Benchmark parameters:")

	tw := tabwriter.NewWriter(os.Stdout, 16, 2, 1, ' ', 0)
	tabLine := func(name string, value interface{}) {
		_, _ = fmt.Fprint(tw, name, "\t", value, "\n")
	}
	tabLine("Endpoint URL", urlHost)
	tabLine("Protocol", protocol)
	tabLine("Host ip", hostIPForPrinting)
	if region != "" {
		tabLine("Region", region)
	}
	tabLine("Bucket", bucket)
	tabLine("Prefix", objPrefix)
	tabLine("Distribute keys", distributeKeys)
	tabLine("Test time", durationSecs)
	tabLine("Test objects", objects)
	tabLine("Threads", threads)
	tabLine("Size", sizeArg)
	tabLine("Loops", loops)
	if useMultipart {
		tabLine("Multipart",
			fmt.Sprintf("%s per part, %d parallel uploads", multipartSizeArg, multipartConcurrency))
	} else {
		tabLine("Multipart", "false")
	}
	tabLine("Max retries", maxRetries)
	_ = tw.Flush()
	fmt.Println()

	// Test access to the bucket
	err = client.Prepare(bucket)
	if err != nil {
		log.Fatalf("error preparing the bucket: %v\n"+
			"For more information, run again with flag -v.", bucket)
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

	fmt.Printf("Starting loop %d...\n", loop)

	// Run the upload case
	successfulUploadIDs = make([]int, 0, 1000) // reused in download step
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
		successfulUploadIDs = append(successfulUploadIDs, v.Id)
		uploadedBytes += objectSize
		uploadDurations = append(uploadDurations, v.Duration.Seconds())
		totalDuration += v.Duration.Seconds()
	}

	// log.Info(successfulUploadIDs)
	successfulUploads := len(successfulUploadIDs)
	failedUploads := len(uploads) - len(successfulUploadIDs)
	uploadMBps := (float64(uploadedBytes) / uploadTime) / (1000 * 1000) // TODO: make base 2 for uniformity
	uploadPerSecond := float64(successfulUploads) / uploadTime

	fmt.Printf("%-9s%-6s%-11s%-7s%-12s%-8s%-8s%-12s\n",
		"Threads", "Size", "Operation", "Time", "Successful", "Failed", "MBps", "OPps")
	fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-8.2f%-12.2f\n",
		threads, bytefmt.ByteSize(objectSize), "PUT", uploadTime, successfulUploads, failedUploads, uploadMBps, uploadPerSecond)
	printHistogram(uploads)

	if len(successfulUploadIDs) < 5 {
		if verbose == false {
			log.Fatal("Not enough successful uploads to continue. For more information, run again with flag -v")
		} else {
			log.Fatal("Not enough successful uploads to continue.")
		}
	}

	if pauseBetweenPhases {
		pause()
	}

	if !noGet {
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

		if successfulDownloads == 0 {
			log.Fatal("All downloads failed\n" +
				"For more information, run again with flag -v.")
		}

		mbPs := (float64(downloadedBytes) / downloadTime) / (1000 * 1000)
		downloadsPerSecond := float64(successfulDownloads) / downloadTime

		fmt.Printf("%-9d%-6v%-11s%-7.2f%-12v%-8v%-8.2f%-12.2f\n",
			threads, bytefmt.ByteSize(objectSize), "GET", downloadTime, successfulDownloads, failedDownloads, mbPs, downloadsPerSecond)
		printHistogram(downloads)
	}

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

	deleteStart := time.Now()
	deleteEg, deleteCtx := errgroup.WithContext(context.Background())
	for n := 0; n < threads; n++ {
		deleteEg.Go(func() error {
			for {
				select {
				case idx, ok := <-deleteChan:
					if !ok {
						return nil
					}
					err := client.DoDelete(deleteCtx, objectKey(idx))
					if err != nil {
						return err
					}
				case <-deleteCtx.Done():
					return deleteCtx.Err()
				}
			}
		})
	}
	deleteEg.Go(func() error {
		for i, v := range successfulUploadIDs {
			select {
			case deleteChan <- v:
			case <-deleteCtx.Done():
				close(deleteChan)
				return deleteCtx.Err()
			}
			if i > 0 && i%100000 == 0 {
				fmt.Printf("%d deletes completed\n", i)
			}
		}
		close(deleteChan)
		return nil
	})
	if err := deleteEg.Wait(); err != nil {
		log.Fatal("Error while performing delete: ", err)
	}
	dDelete := time.Since(deleteStart)
	fmt.Printf("Deletes completed in %v\n", dDelete)
}

func printHistogram(res []TransferResult) {
	histogram := hdrhistogram.New(0, time.Hour.Milliseconds(), 0)
	for _, re := range res {
		_ = histogram.RecordValue(re.Duration.Milliseconds())
	}
	_ = PrintHistogram(histogram, os.Stdout)
}

// Adapted from h.PrintHistogram
func PrintHistogram(h *hdrhistogram.Histogram, w io.Writer) (err error) {
	valueScale := 1.0
	outputWriter := tabwriter.NewWriter(w, 4, 2, 2, ' ', 0)
	dist := h.CumulativeDistributionWithTicks(1)
	_, err = outputWriter.Write([]byte("Percentile\tLatency(ms)\tCount\tCumulative\n"))
	if err != nil {
		return
	}

	var prevCount int64
	for _, slice := range dist {
		percentile := slice.Quantile / 100.0
		_, err = outputWriter.Write([]byte(fmt.Sprintf("%12f\t%12.0f\t%12d\t%12d\n", percentile, float64(slice.ValueAt)/valueScale, slice.Count-prevCount, slice.Count)))
		if err != nil {
			return
		}
		prevCount = slice.Count
	}

	err = outputWriter.Flush()
	if err != nil {
		return
	}

	footer := fmt.Sprintf("#[Mean    = %12.3f, StdDeviation   = %12.3f]\n#[Max     = %12.3f, Total count    = %12d]\n",
		h.Mean()/valueScale,
		h.StdDev()/valueScale,
		float64(h.Max())/valueScale,
		h.TotalCount(),
	)
	_, err = w.Write([]byte(footer))
	return
}

func runAndCollectResults(indexes chan<- int, res <-chan TransferResult) []TransferResult {
	var (
		nextId         int
		successUploads uint64
	)

	// start first upload
	for nextId = 0; nextId < threads+1; nextId++ {
		indexes <- nextId
	}
	results := make([]TransferResult, 0, objects)

	// set timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(durationSecs))
	defer timeoutCancel()

	// start collector
	timingsChan := make(chan timings, 1000)
	if detailed {
		go func() {
			_ = collectLoop(timeoutCtx, time.Second, timingsChan)
		}()
	}

	// create histogram, use millis resolution

Loop:
	for {
		select {
		case <-timeoutCtx.Done():
			break Loop
		case r := <-res:
			if detailed {
				timingsChan <- timings{
					copy:  r.Duration,
					bytes: int64(objectSize),
				}
			}
			results = append(results, r)
			if r.Error != nil {
				indexes <- r.Id
			} else {
				successUploads++
				// break out after N objects
				if successUploads == objects {
					break Loop
				}
				indexes <- nextId
				nextId++
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

func runUpload(ctx context.Context, ids <-chan int, res chan<- TransferResult) {
	for id := range ids {
		reader := bytes.NewReader(objectData)

		startTime := time.Now()
		r := client.DoUpload(ctx, objectKey(id), reader)

		r.Duration = time.Since(startTime)
		r.Id = id

		if r.Error != nil {
			if !strings.Contains(r.Error.Error(), "context canceled") {
				log.Error(r.Error)
			}
		}

		res <- r
	}
}

func objectKey(idx int) string {
	if !distributeKeys {
		return fmt.Sprintf("%s/%d", objPrefix, idx)
	}

	// hash the index
	bin := make([]byte, 8)
	binary.LittleEndian.PutUint64(bin, uint64(idx))
	sum := md5.Sum(bin)
	hexStr := hex.EncodeToString(sum[:])

	// format hex string to xx/yy/zzzzzzzzzzzzzzzzzzzzzzzzzzzz
	b := strings.Builder{}
	b.WriteString(objPrefix)
	b.WriteString("/")
	b.WriteString(hexStr[:2])
	b.WriteString("/")
	b.WriteString(hexStr[2:4])
	b.WriteString("/")
	b.WriteString(hexStr[4:])

	return b.String()
}

func runDownload(ctx context.Context, indexes <-chan int, res chan<- TransferResult) {
	for id := range indexes {
		idx := successfulUploadIDs[id%len(successfulUploadIDs)]

		startTime := time.Now()
		r := client.DoDownload(ctx, objectKey(idx))

		r.Duration = time.Since(startTime)
		r.Id = id

		if r.Error != nil {
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
