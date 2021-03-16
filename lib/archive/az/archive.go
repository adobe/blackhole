/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

// Package file provides functionality to read and write to a archive file of
// HTTP requests. Requests are serialized using Flatbuffers (See request.fbs for schema)
// Optionally data can be compressed using LZ4. LZ4 provides a CPU friendly compression
package az

import (
	"context"
	"fmt"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/pkg/errors"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"sync"
	"time"
	// "github.com/gogo/protobuf/proto"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var gAZSession struct {
	sync.Mutex  // Used only for writing. Not for reading
	accountName string
	azPipeline  pipeline.Pipeline
}

var azUrlRegex = regexp.MustCompile("([^/:]+)://([^/]+)/(.*?)$")

type AZArchive struct {
	common.BasicArchive
	containerName string
	s3SubDir      string
}

func azInit() (err error) {
	gAZSession.Lock() // gAZSession is goroutine safe only after it has been instantiated
	defer gAZSession.Unlock()
	if gAZSession.azPipeline == nil {

		accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
		gAZSession.accountName = accountName
		if len(accountName) == 0 || len(accountKey) == 0 {
			return errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
		}

		// Create a default request pipeline using your storage account name and account key.
		credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return errors.Wrapf(err, "Invalid credentials")
		}

		gAZSession.azPipeline = azblob.NewPipeline(credential,
			azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: time.Minute * 10}})

	}
	return err
}

// NewArchive creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*AZArchive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, compress bool, bufferSize int) (rf *AZArchive, err error) {

	err = azInit()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize azure connection")
	}

	// https://play.golang.org/p/vZ4NZzi6vrK
	parts := azUrlRegex.FindStringSubmatch(outDir)
	if len(parts) != 4 { // must be exactly 4 parts
		return nil, errors.Wrap(err, "Unable to parse azure blob url format")
	}
	containerName, directory := parts[2], parts[3]

	rf = &AZArchive{BasicArchive: *common.NewBasicArchive(
		"",
		prefix, extension, compress, bufferSize,
		rf.finalizeArchive),
		containerName: containerName,
		s3SubDir:      directory}

	err = rf.Rotate()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// OpenArchive opens an archive file for reading. `*AZArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *AZArchive, err error) {

	rfi, err := common.OpenArchive(fileName, bufferSize)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}
	return &AZArchive{BasicArchive: *rfi}, nil
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will upload to Azure Blobstore.
func (rf *AZArchive) finalizeArchive(filepath string, fileSize int64) (err error) {

	finalName := fmt.Sprintf("%s/%s", rf.s3SubDir, path.Base(rf.Name()))

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", gAZSession.accountName, rf.containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	azContainerURL := azblob.NewContainerURL(*URL, gAZSession.azPipeline)

	blockBlobURL := azContainerURL.NewBlockBlobURL(finalName)

	finalFP, err := os.Open(filepath)
	if err != nil {
		err = errors.Wrapf(err, "unable to reopen archive file: %s", finalName)
		return err
	}
	defer finalFP.Close()

	log.Printf("L:%s => AZ:%s [BEGIN]", filepath, finalName)

	var statChan = make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go common.UploadProgressPrinter(statChan, filepath, fileSize, &wg)

	_, err = azblob.UploadFileToBlockBlob(context.Background(), finalFP, blockBlobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 4,
		Progress: func(bytesTransferred int64) {
			statChan <- bytesTransferred
		}})
	if err != nil {
		err = errors.Wrapf(err, "ERROR: Blobstore upload error for: %s", filepath)
		return err
	}
	log.Printf("L:%s => AZ:%s [END]", filepath, finalName)

	err = os.Remove(filepath)
	if err != nil {
		return errors.Wrapf(err, "unable to remove archive file %s after uploading to azure", filepath)
	}

	close(statChan)
	wg.Wait() // Waiting for status monitor to exit
	rf.Reset()

	return nil
}
