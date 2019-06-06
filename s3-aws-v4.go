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
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
)

var discarder = &DiscardWriterAt{}

type S3AwsV4 struct {
	S3           *s3.S3
	Bucket       string
	UseMultipart bool
	MPUploader   *s3manager.Uploader
	MPDownloader *s3manager.Downloader
}

func NewS3AwsV4(access_key, secret_key, url_host, region string) *S3AwsV4 {
	awsConfig := &aws.Config{
		Credentials: credentials.NewStaticCredentials(
			access_key,
			secret_key,
			""),
		Endpoint:                aws.String(url_host),
		Region:                  aws.String(region),
		DisableSSL:              aws.Bool(true),
		DisableComputeChecksums: aws.Bool(true),
		S3ForcePathStyle:        aws.Bool(true),
		MaxRetries:              aws.Int(maxRetries),
		HTTPClient:              httpClient,
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		log.Fatal("error initializing s3v4 client ", err)
	}

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = int64(part_size)
		u.Concurrency = multipartConcurrency
	})

	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = int64(part_size)
		d.Concurrency = multipartConcurrency
	})

	return &S3AwsV4{
		S3:           s3.New(sess),
		MPUploader:   uploader,
		MPDownloader: downloader,
	}
}

func (u *S3AwsV4) Prepare(bucket string) error {
	u.Bucket = bucket
	_, err := u.S3.HeadBucket(&s3.HeadBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		log.Fatal("unable to access the bucket ", err)
	}
	return err
}

func (u *S3AwsV4) DoDelete(ctx context.Context, id int) error {
	key := fmt.Sprintf("%s-%d", objPrefix, id)

	_, err := u.S3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: &u.Bucket,
		Key:    &key,
	})
	if err != nil {
		log.Errorf("Error deleting object %s: %v", key, err)
	}
	return err
}

func (u *S3AwsV4) DoDownload(ctx context.Context, id int) (result TransferResult) {
	var err error
	var getObjRes *s3.GetObjectOutput
	var copied int64

	key := fmt.Sprintf("%s-%d", objPrefix, id)

	getObjInput := s3.GetObjectInput{
		Bucket: &u.Bucket,
		Key:    &key,
	}

	if u.UseMultipart {
		_, err = u.MPDownloader.DownloadWithContext(ctx, discarder, &getObjInput)
	} else {
		getObjRes, err = u.S3.GetObjectWithContext(ctx, &getObjInput)
	}

	if err != nil {
		result.Error = fmt.Errorf("error downloading object %s: %v", key, err)
		return
	}

	if !u.UseMultipart {
		// manually receive the file
		copied, err = io.Copy(ioutil.Discard, getObjRes.Body)

		// set the duration again
		if err != nil {
			result.Error = fmt.Errorf("error receiving response %v", err.Error())
			return
		}

		if uint64(copied) != object_size {
			result.Error = fmt.Errorf("wrong response size")
			return
		}
	}

	return
}

func (u *S3AwsV4) DoUpload(ctx context.Context, id int, data io.ReadSeeker) (result TransferResult) {
	key := fmt.Sprintf("%s-%d", objPrefix, id)

	var err error

	if u.UseMultipart {
		uInput := s3manager.UploadInput{
			Body:   data,
			Bucket: &u.Bucket,
			Key:    &key,
		}
		_, err = u.MPUploader.UploadWithContext(ctx, &uInput)
	} else {
		putObjInput := s3.PutObjectInput{
			Body:   data,
			Bucket: &u.Bucket,
			Key:    &key,
		}
		_, err = u.S3.PutObjectWithContext(ctx, &putObjInput)
	}

	result.Id = id

	if err != nil {
		result.Error = fmt.Errorf("error uploading object %s: %v", key, err)
		return
	}

	return
}

type DiscardWriterAt struct {
}

func (d *DiscardWriterAt) Write(p []byte) (int, error) {
	return ioutil.Discard.Write(p)
}

func (d *DiscardWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	return ioutil.Discard.Write(p)
}
