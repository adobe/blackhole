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

// Package az provides archive interface for Azure blob-store
package az

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/pkg/errors"
	"go.uber.org/zap"

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
	contSubDir    string
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
// File is uploaded automatically to azure blobstore on `.Close()` call.
// `*AZArchive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, options ...func(*common.BasicArchive) error) (rf *AZArchive, err error) {

	err = azInit()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize azure connection")
	}

	// https://play.golang.org/p/vZ4NZzi6vrK
	parts := azUrlRegex.FindStringSubmatch(outDir)
	if len(parts) != 4 { // must be exactly 4 parts
		return nil, errors.New("Unable to parse azure blob url format")
	}
	containerName, directory := parts[2], parts[3]

	ba, err := common.NewBasicArchive(
		"",
		prefix, extension, options...)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to initialize basic archive")
	}
	rf = &AZArchive{BasicArchive: *ba,
		containerName: containerName,
		contSubDir:    directory}
	rf.Finalizer = rf.finalizeArchive

	err = rf.Rotate()
	if err != nil {
		return nil, err
	}
	return rf, err
}

func getContainer(fullPath string) (cu azblob.ContainerURL, rest string, err error) {

	err = azInit()
	if err != nil {
		return cu, "", errors.Wrap(err, "Unable to initialize azure connection")
	}

	parts := azUrlRegex.FindStringSubmatch(fullPath)
	if len(parts) != 4 { // must be exactly 4 parts
		return cu, "", errors.Wrap(err, "Unable to parse azure blob url format")
	}

	containerName, rest := parts[2], parts[3]

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", gAZSession.accountName, containerName))

	cu = azblob.NewContainerURL(*URL, gAZSession.azPipeline)
	return cu, rest, err

}

// OpenArchive opens an archive file for reading. `*AZArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *AZArchive, err error) {

	azContainerURL, filePath, err := getContainer(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize azure connection")
	}

	blobURL := azContainerURL.NewBlobURL(filePath)

	fp, err := ioutil.TempFile("", fmt.Sprintf("tmp_*_%s", path.Base(filePath)))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create temp file")
	}
	props, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to stat blobstore file")
	}
	fileSize := props.ContentLength()

	rf.Logger.Debug("Azure Download [BEGIN]",
		zap.String("local", filePath),
		zap.Int64("size", fileSize),
		zap.String("remote", fp.Name()))

	var statChan = make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go rf.ProgressPrinter(statChan, filePath, fileSize, &wg)

	err = azblob.DownloadBlobToFile(context.Background(), blobURL, 0, 0, fp, azblob.DownloadFromBlobOptions{
		Progress: func(bytesTransferred int64) {
			statChan <- bytesTransferred
		}})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to download file: %s", filePath)
	}

	close(statChan)
	wg.Wait() // Waiting for status monitor to exit

	rf.Logger.Debug("Azure Download [END]",
		zap.String("local", filePath),
		zap.Int64("size", fileSize),
		zap.String("remote", fp.Name()))

	rfi, err := common.OpenArchive(fp.Name(), bufferSize, true)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}
	return &AZArchive{BasicArchive: *rfi}, nil
}

func List(dir string) (files []string, err error) {

	azContainerURL, subDir, err := getContainer(dir)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize azure connection")
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := azContainerURL.ListBlobsFlatSegment(context.Background(), marker,
			azblob.ListBlobsSegmentOptions{Prefix: subDir})
		if err != nil {
			return nil, errors.Wrap(err, "Unable to list azure connection")
		}
		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			files = append(files, blobInfo.Name)
		}
	}

	return files, err
}

func Delete(dir string, files []string) (err error) {

	azContainerURL, _, err := getContainer(dir)
	if err != nil {
		return errors.Wrap(err, "Unable to initialize azure connection")
	}

	// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
	for _, fileName := range files {
		blobURL := azContainerURL.NewBlobURL(fileName)
		_, err = blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		if err != nil {
			return errors.Wrapf(err, "Unable to delete azure blob: %s", fileName)
		}
		fmt.Printf("DELETED: %s\n", fileName)
	}

	return err
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will upload to Azure Blobstore.
func (rf *AZArchive) finalizeArchive() (finalFile string, err error) {

	filePath := rf.Name()
	fi, err := os.Stat(filePath)
	if err != nil {
		err = errors.Wrapf(err, "unable to stat file %s", filePath)
		return "", err
	}
	fileSize := fi.Size()
	finalPath := path.Join(rf.contSubDir, path.Base(filePath))
	finalPath = strings.TrimSuffix(finalPath, ".tmp")
	finalFile = path.Base(finalPath) // Only filename part

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", gAZSession.accountName, rf.containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	azContainerURL := azblob.NewContainerURL(*URL, gAZSession.azPipeline)

	blockBlobURL := azContainerURL.NewBlockBlobURL(finalPath)

	finalFP, err := os.Open(filePath)
	if err != nil {
		err = errors.Wrapf(err, "unable to reopen archive file: %s", finalPath)
		return "", err
	}
	defer finalFP.Close()

	rf.Logger.Debug("Azure Upload [BEGIN]",
		zap.String("local", filePath),
		zap.String("remote", finalPath))

	var statChan = make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go rf.ProgressPrinter(statChan, filePath, fileSize, &wg)

	_, err = azblob.UploadFileToBlockBlob(context.Background(), finalFP, blockBlobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 4,
		Progress: func(bytesTransferred int64) {
			statChan <- bytesTransferred
		}})
	if err != nil {
		err = errors.Wrapf(err, "ERROR: Blobstore upload error for: %s", filePath)
		return "", err
	}
	rf.Logger.Info("Azure Upload [END]",
		zap.String("local", filePath),
		zap.String("remote", finalPath),
		zap.Int64("content-bytes", rf.TrueContentLength()),
		zap.Int64("compressed-bytes", fileSize))

	err = os.Remove(filePath)
	if err != nil {
		return "", errors.Wrapf(err, "unable to remove archive file %s after uploading to azure", filePath)
	}

	close(statChan)
	wg.Wait() // Waiting for status monitor to exit

	return finalFile, nil
}
