## Introduction

`rs-benchmark` is a performance testing tool provided by [RStor Inc](https://rstor.io/) to effectively compare different object storages with different protocols. This tool is an improvement of [the one](https://github.com/wasabi-tech/s3-benchmark) developed by Wasabi-Tech.

This tool is provided to independently benchmark RStorage S3 service provided by RStor Inc, as well as the other major CSP’s object storage services like Amazon S3, Azure Blob Storage, Google Cloud Storage, as well as others that rely on the same protocols.

We decided to open-source `rs-benchmark` because we found out that other s3-benchmark tools don't correctly compute test results: when a `GET` test is performed, the tools only consider as error a bad `HTTP` request, i.e. they don't catch server errors. For example, if the server returns an error, e.g. `404` after requesting a non-existent object, the tools will ignore it and the test is considered passed, thus incorrectly measuring a higher throughput.

## Building

#### Requirements
- `git`
- The Go compiler, `go` version 1.11 or greater
- A reasonably recent Linux distro

#### Install Go compiler on Linux

Ubuntu 18.04

First add the repository:
```
sudo add-apt-repository ppa:gophers/archive && \
sudo apt-get update
```
Then, install the Go package:
```
sudo apt-get install golang-1.11-go
```
Finally, update the `$PATH` variable:
```
echo 'export PATH="/usr/lib/go-1.11/bin:$PATH"' >> ~/.bashrc && \
source ~/.bashrc
```

Debian

On Debian Stretch, you will need to enable backports (https://backports.debian.org/Instructions/) and install with:

```
sudo apt-get -t stretch-backports install golang
```

CentOS 7 / RHEL 7

```
sudo yum install epel-release
sudo yum install go
```

Fedora 29/30

```
sudo dnf install golang
```

If you cannot find a package for your distro, follow the instructions at https://golang.org/doc/install

#### Compiling

Clone the repository with
```
git clone https://github.com/RstorLabs/rs-benchmark.git
```
Now run
```
cd rs-benchmark && go build
```
The executable `rs-benchmark` should now be visible in the project folder.

## Usage

Syntax:
```
./rs-benchmark [OPTIONS]
```
Below are the available command line options to the program:

```
  -a string
    	Access key
  -b string
    	Bucket for testing
  -d int
    	Duration of each test in seconds (default 60)
  -h, --help
        Show help screen
  -ip string
    	forces all hostnames to resolve to this address (s3v2, s3v4 signing protocol only)
  -l int
    	Number of times to repeat test (default 1)
  -maxRetries int
    	number of retries on failure (default 0. s3v4 only)
  -multipart
    	use multipart (s3v4 only)
  -multipart-concurrency int
    	concurrency to use for multipart requests (default 5)
  -multipart-size string
    	Size of the multipart chunks (default "5M")
  -pause
    	whether to pause between phases
  -prefix string
    	will create objects with key: 'prefix-number' (default "Object")
  -protocol string
    	client protocol: s3v2, s3v4, azure, gcp
  -r string
    	Region for testing
  -s string
    	Secret key
  -t int
    	Number of parallel requests to run (default 1)
  -u string
    	URL for endpoint with method prefix (e.g. https://s3.YOUR_CUSTOMER_NAME.rstorcloud.io)
  -v	Verbose error output
  -version
        Show version
  -z string
    	Size of objects in bytes with suffix K, M, and G (default "1M")

```

At a bare minimum you need to specify: `-a` (access key), `-s` (secret key), `-b` (test bucket), `-u` (endpoint) and `-protocol` (client protocol).

## Examples

Suppose we want to benchmark the RStorage S3 service, using a bucket named `testbucket`, having object size of 10MB, for 90 seconds, using 4 parallel transfers (the number of files to be uploaded in parallel). Then, we would run

```bash
./rs-benchmark \
    -a ACCESS_KEY \
    -s SECRET_KEY \
    -b testbucket \
    -u https://s3.YOUR_CUSTOMER_NAME.rstorcloud.io \
    -t 4 \
    -z 10M \
    -d 90 \
    -r any \
    -protocol s3v4
```

You can create and manage `ACCESS_KEY` and `SECRET_KEY` by yourself through our Customer UI.


If we wanted to benchmark the object storage service from another provider, the `-u` and `-protocol` flags need to be changed with the appropriate host URL and client protocol. For example, if we wanted to run a similar benchmark on Azure, using the same parameters as before, we would run

```bash
./rs-benchmark \
    -a ACCOUNT_NAME \
    -s ACCOUNT_KEY \
    -b testbucket \
    -u https://ACCOUNT_NAME.blob.core.windows.net \
    -t 4 \
    -z 10M \
    -d 90 \
    -r any \
    -protocol azure
```

The same test against Amazon S3 would be:


```bash
./rs-benchmark \
    -a ACCESS_KEY \
    -s SECRET_KEY \
    -b testbucket \
    -u https://s3.amazonaws.com \
    -t 4 \
    -z 10M \
    -d 90 \
    -r any \
    -protocol s3v4
```

You can test also different combinations of multipart parallel transfers with different chunk sizes. For example, if you wanted to test a 5GB upload with 100MB chunk size and 3 parallel connections (the number of multipart chunks to be uploaded in parallel), against RStorage, you would run

```bash
./rs-benchmark \
    -a ACCESS_KEY \
    -s SECRET_KEY \
    -b testbucket \
    -u https://s3.YOUR_CUSTOMER_NAME.rstorcloud.io \
    -multipart \
    -multipart-concurrency 3 \
    -multipart-size "100M" \
    -t 4 \
    -z 5G \
    -d 90 \
    -r any \
    -protocol s3v4
```

In this last example, the tool will upload 12 chunks in parallel (`multipart-concurrency = 3` chunks times `threads = 4` files).

To increase accuracy of test results, you can tell `rs-benchmark` to repeat the test multiple times with the option `-l`.

## Additional notes

#### Azure Blob Storage
To get started with Azure benchmarking, first obtain credentials from https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal. Access key is your account name. The Host URL is in the form of `https://ACCOUNT_NAME.blob.core.windows.net`.

#### Google Cloud Storage
Authentication happens at instance level, so you must run the test from a Google cloud instance, which has been authorized to access the storage. Alternatively, you can use the S3 compatibility layer, with the `s3v4` protocol. 

#### Caveats on multipart
Multipart tests are enabled only for `s3v4` protocol. Azure has a chunk size limit of 128MB, while Google Cloud Platform has a limit of 32 chunks per multipart upload. These limits make an apple-to-apple comparison difficult, therefore the `-multipart-concurrency` parameter is automatically disabled when used in combination with `-protocol azure` and `gcp`.
