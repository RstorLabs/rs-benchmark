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
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
)

type AzureUploader struct {
	ContainerUrl azblob.ContainerURL
	ServiceUrl   azblob.ServiceURL
	UseMultipart bool
}

func NewAzureUploader(accessKey, secretKey, urlHost string) *AzureUploader {
	credential, err := azblob.NewSharedKeyCredential(accessKey, secretKey)
	if err != nil {
		log.Fatal(err)
	}

	// https://github.com/ncw/rclone/issues/2647#issuecomment-435480482
	pipelineOpts := azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: time.Hour * 6}}
	p := azblob.NewPipeline(credential, pipelineOpts)

	u, err := url.Parse(urlHost) // https://$ACCOUNT_NAME.blob.core.windows.net
	if err != nil {
		log.Fatal(err)
	}

	serviceURL := azblob.NewServiceURL(*u, p)

	return &AzureUploader{
		ServiceUrl: serviceURL,
	}
}

func (u *AzureUploader) Prepare(bucket string) error {
	u.ContainerUrl = u.ServiceUrl.NewContainerURL(bucket)

	// TODO: test if connection works
	// Create the container on the service (with no metadata and no public access)
	ctx := context.Background()

	_, err := u.ContainerUrl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		// no idea how to check bucket exists, let's just log the error
		log.Errorf("unable to create bucket %s", bucket)
	}

	return nil
}

func (u *AzureUploader) DoDelete(ctx context.Context, key string) error {
	blobURL := u.ContainerUrl.NewBlockBlobURL(key)

	_, err := blobURL.Delete(ctx,
		azblob.DeleteSnapshotsOptionNone,
		azblob.BlobAccessConditions{})

	if err != nil {
		log.Errorf("Error deleting object %s: %v", key, err)
	}
	return err
}

func (u *AzureUploader) DoDownload(ctx context.Context, key string) (result TransferResult) {
	blobURL := u.ContainerUrl.NewBlockBlobURL(key)

	get, err := blobURL.Download(ctx, 0, 0,
		azblob.BlobAccessConditions{}, false)

	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		result.Error = fmt.Errorf("error downloading object %s: %v", key, err)
		return
	}

	reader := get.Body(azblob.RetryReaderOptions{})

	// Receive response
	copied, err := io.Copy(ioutil.Discard, reader)
	_ = reader.Close()

	if err != nil {
		result.Error = fmt.Errorf("error receiving response %v", err.Error())
		return
	}

	if uint64(copied) != objectSize {
		result.Error = fmt.Errorf("wrong response size")
		return
	}

	return
}

func (u *AzureUploader) DoUpload(ctx context.Context, key string, data io.ReadSeeker) (result TransferResult) {
	var err error
	blobURL := u.ContainerUrl.NewBlockBlobURL(key)

	// const maxSinglePartSize = 100 * 1000 * 1000
	var multipartPartSize = partSize

	if !u.UseMultipart {
		_, err := blobURL.Upload(ctx, data,
			azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"},
			azblob.Metadata{},
			azblob.BlobAccessConditions{})

		if err != nil {
			result.Error = fmt.Errorf("error uploading object %s: %v", key, err)
			return
		}

		return
	}

	nParts := numParts(objectSize, multipartPartSize)
	base64BlockIDs := make([]string, 0, nParts)
	blockIdx := 0
	for sent := uint64(0); sent < objectSize; {
		partEnd := sent + multipartPartSize

		if partEnd > objectSize {
			partEnd = objectSize
		}

		base64BlockIDs = append(base64BlockIDs, blockIDIntToBase64(blockIdx))
		part := objectData[sent:partEnd]

		_, err := blobURL.StageBlock(ctx, base64BlockIDs[blockIdx],
			bytes.NewReader(part), azblob.LeaseAccessConditions{},
			nil)

		if err != nil {
			result.Error = fmt.Errorf("error staging part for %s: %v", key, err)
			return
		}

		sent = partEnd
		blockIdx++
	}

	// After all the blocks are uploaded, atomically commit them to the blob.
	_, err = blobURL.CommitBlockList(ctx, base64BlockIDs, azblob.BlobHTTPHeaders{},
		azblob.Metadata{}, azblob.BlobAccessConditions{})

	if err != nil {
		result.Error = fmt.Errorf("error committing block list for %s: %v", key, err)
		return
	}

	return
}

// These helper functions convert a binary block ID to a base-64 string and vice versa
// NOTE: The blockID must be <= 64 bytes and ALL blockIDs for the block must be the same length
func blockIDBinaryToBase64(blockID []byte) string {
	return base64.StdEncoding.EncodeToString(blockID)
}

func blockIDBase64ToBinary(blockID string) []byte {
	bin, _ := base64.StdEncoding.DecodeString(blockID)
	return bin
}

// These helper functions convert an int block ID to a base-64 string and vice versa
func blockIDIntToBase64(blockID int) string {
	binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return blockIDBinaryToBase64(binaryBlockID)
}

func blockIDBase64ToInt(blockID string) int {
	blockIDBase64ToBinary(blockID)
	return int(binary.LittleEndian.Uint32(blockIDBase64ToBinary(blockID)))
}

// https://stackoverflow.com/a/29925587
func numParts(n uint64, multiple uint64) uint64 {
	return (n + multiple - 1) / multiple
}
