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
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type S3AwsV2 struct {
	AccessKey string
	SecretKey string
	Bucket    string
	Host      string
}

func NewS3AwsV2(access_key, secret_key, url_host, region string) *S3AwsV2 {
	return &S3AwsV2{
		AccessKey: access_key,
		SecretKey: secret_key,
		Host:      url_host,
	}
}

func (u *S3AwsV2) Prepare(bucket string) error {
	u.Bucket = bucket
	return nil
}

func (u *S3AwsV2) DoDelete(ctx context.Context, id int) error {
	key := fmt.Sprintf("%s-%d", objPrefix, id)
	path := fmt.Sprintf("%s/%s/%s", u.Host, u.Bucket, key)

	req, _ := http.NewRequest("DELETE", path, nil)

	setSignature(req, u.AccessKey, u.SecretKey)

	resp, err := httpClient.Do(req)

	if err != nil {
		log.Errorf("Error deleting object %s: %v", path, err)
	} else if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
		log.Errorf("Error deleting %s", path)
	}

	return err
}

func (u *S3AwsV2) DoDownload(ctx context.Context, id int) (result TransferResult) {
	key := fmt.Sprintf("%s-%d", objPrefix, id)
	path := fmt.Sprintf("%s/%s/%s", u.Host, u.Bucket, key)

	req, _ := http.NewRequest("GET", path, nil)
	req = req.WithContext(ctx)
	setSignature(req, u.AccessKey, u.SecretKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("error downloading object %s: %v", path, err)
		return
	}

	if resp.StatusCode != 200 {
		if resp.StatusCode == http.StatusServiceUnavailable {
			result.Error = fmt.Errorf("slowdown requested")
		} else {
			result.Error = fmt.Errorf("non-ok status %d, %v", resp.StatusCode, resp.Status)
		}

		if resp.Body != nil {
			_, _ = io.Copy(ioutil.Discard, resp.Body)
			_ = resp.Body.Close()
		}

		return
	}

	if resp.Body == nil {
		result.Error = fmt.Errorf("empty body")
		return
	}

	// Receive response
	copied, err := io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		result.Error = fmt.Errorf("error receiving response %v", err.Error())
		return
	}

	_ = resp.Body.Close()

	if uint64(copied) != object_size {
		result.Error = fmt.Errorf("wrong response size")
		return
	}

	return
}

func (u *S3AwsV2) DoUpload(ctx context.Context, id int, data io.ReadSeeker) (result TransferResult) {
	key := fmt.Sprintf("%s-%d", objPrefix, id)
	path := fmt.Sprintf("%s/%s/%s", u.Host, u.Bucket, key)

	result.Id = id
	req, _ := http.NewRequest("PUT", path, data)
	req = req.WithContext(ctx)
	req.Header.Set("Content-Length", strconv.FormatUint(object_size, 10))

	// req.Header.Set("Content-MD5", object_data_md5)
	setSignature(req, u.AccessKey, u.SecretKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("error uploading object %s: %v", path, err)
		return result
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusServiceUnavailable {
			result.Error = fmt.Errorf("slowdown requested")
		} else {
			var body []byte
			if resp.Body != nil {
				body, _ = ioutil.ReadAll(resp.Body)
			}
			result.Error = fmt.Errorf("not-ok status, received %d, %s, %s",
				resp.StatusCode, resp.Status, string(body))
		}
	}

	return result
}

func parseAmzHeaders(req *http.Request) string {
	var headers []string
	for header := range req.Header {
		norm := strings.ToLower(strings.TrimSpace(header))
		if strings.HasPrefix(norm, "x-amz") {
			headers = append(headers, norm)
		}
	}

	sort.Strings(headers)
	for n, header := range headers {
		headers[n] = header + ":" + strings.Replace(req.Header.Get(header), "\n", " ", -1)
	}
	if len(headers) > 0 {
		return strings.Join(headers, "\n") + "\n"
	} else {
		return ""
	}
}

func hmacSHA1(key []byte, content string) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(content))
	return mac.Sum(nil)
}

func setSignature(req *http.Request, accessKey, secretKey string) {
	dateHdr := time.Now().UTC().Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", dateHdr)
	parsedResource := req.URL.EscapedPath()
	parsedHeaders := parseAmzHeaders(req)
	stringToSign := req.Method + "\n" + req.Header.Get("Content-MD5") + "\n" + req.Header.Get("Content-Type") + "\n\n" +
		parsedHeaders + parsedResource
	hash := hmacSHA1([]byte(secretKey), stringToSign)
	signature := base64.StdEncoding.EncodeToString(hash)
	req.Header.Set("Authorization", fmt.Sprintf("AWS %s:%s", accessKey, signature))
}
