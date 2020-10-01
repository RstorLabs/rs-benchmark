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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"

	gstorage "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type GCP struct {
	Client       *gstorage.Client
	Bucket       *gstorage.BucketHandle
	BucketName   string
	UseMultipart bool
	MPUploader   *s3manager.Uploader
	MPDownloader *s3manager.Downloader
}

func NewGCP(access_key, secret_key, url_host, region string) *GCP {
	ctx := context.TODO()
	client, err := gstorage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return &GCP{
		Client: client,
	}
}

func (u *GCP) Prepare(bucketName string) error {
	u.Bucket = u.Client.Bucket(bucketName)
	return nil
}

func (u *GCP) DoDelete(ctx context.Context, id int) error {
	key := fmt.Sprintf("%s-%d", objPrefix, id)

	err := u.Bucket.Object(key).Delete(ctx)
	if err != nil {
		log.Errorf("Error deleting object %s: %v", key, err)
	}
	return err
}

func (u *GCP) DoDownload(ctx context.Context, id int) (result TransferResult) {
	var err error
	var copied int64

	key := fmt.Sprintf("%s-%d", objPrefix, id)

	objReader, err := u.Bucket.Object(key).NewReader(ctx)

	if err != nil {
		result.Error = fmt.Errorf("error downloading object %s: %v", key, err)
		return
	}

	// manually receive the file
	copied, err = io.Copy(ioutil.Discard, objReader)

	if err != nil {
		_ = objReader.Close()
		result.Error = fmt.Errorf("error receiving response %v", err.Error())
		return
	}

	err = objReader.Close()

	if err != nil {
		result.Error = fmt.Errorf("error closing object %v", err.Error())
		return
	}

	if uint64(copied) != objectSize {
		result.Error = fmt.Errorf("wrong response size")
		return
	}

	return
}

func (u *GCP) DoUpload(ctx context.Context, id int, data io.ReadSeeker) (result TransferResult) {
	key := fmt.Sprintf("%s-%d", objPrefix, id)

	var err error

	var multipartPartSize = partSize

	if !u.UseMultipart {
		objReader := bytes.NewReader(objectData)

		multipartPartSize = objectSize
		objWriter := u.Bucket.Object(key).NewWriter(ctx)
		_, err = io.Copy(objWriter, objReader)

		if err != nil {
			_ = objWriter.Close()
			result.Error = fmt.
				Errorf("error uploading %s %v", key, err)
			return
		}

		err = objWriter.Close()
		if err != nil {
			result.Error = fmt.
				Errorf("error closing file %s %v", key, err)
			return
		}

		return
	}

	if numParts(objectSize, multipartPartSize) > 32 {
		log.Fatal("can't split in more than 32 parts")
	}

	index := 0
	uploadedObjects := make([]*gstorage.ObjectHandle, 0, 32)
	for sent := uint64(0); sent < objectSize; {
		partEnd := sent + multipartPartSize

		if partEnd > objectSize {
			partEnd = objectSize
		}

		part := objectData[sent:partEnd]

		partKey := fmt.Sprintf("%s_%d", key, index)

		partObject := u.Bucket.Object(partKey)
		partWriter := partObject.NewWriter(ctx)

		partReader := bytes.NewReader(part)
		_, err = io.Copy(partWriter, partReader)

		if err != nil {
			_ = partWriter.Close()
			result.Error = fmt.
				Errorf("error uploading part %d for %s: %v", index, key, err)
			return
		}

		err = partWriter.Close()

		if err != nil {
			result.Error = fmt.
				Errorf("error closing uploaded part %d for %s: %v", index, key, err)
			return
		}

		uploadedObjects = append(uploadedObjects, partObject)

		sent = partEnd
		index++
	}

	composer := u.Bucket.Object(key).ComposerFrom(uploadedObjects...)
	_, err = composer.Run(ctx)

	result.Id = id

	if err != nil {
		result.Error = errors.Wrapf(err, "error composing object %s", key)
		return
	}

	return
}
