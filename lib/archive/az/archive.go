/*
Copyright 2020 Adobe. All rights reserved.
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
	"bufio"
	"context"
	"fmt"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/pierrec/lz4/v3"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
	// "github.com/gogo/protobuf/proto"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var gS3Session struct {
	sync.Mutex  // Used only for writing. Not for reading
	accountName string
	azPipeline  pipeline.Pipeline
}

var s3UrlRegex = regexp.MustCompile("([^/:]+)://([^/]+)/(.*?)$")

type S3Archive struct {
	writing       bool
	fp            *os.File      // Underlying FP. Needed to close and flush after we are done.
	zw            *lz4.Writer   // Used only if compression is enabled.
	zr            *lz4.Reader   // Used only if compression is enabled.
	bw            *bufio.Writer // If set, all writes are buffered
	br            *bufio.Reader // If set, all reads are buffered
	name          string        // name, for debugging/printing only
	FinalName     string
	outDir        string
	containerName string
	blockBlobURL  azblob.BlockBlobURL
	directory     string
	prefix        string
	extension     string
	compress      bool
	bufferSize    int
	bytesWritten  int64 // to see if file is empty at Close (during finalize)
}

func (rf *S3Archive) Name() string {
	return rf.name
}

// Write satisfies io.Writer interface - main logic is the transparent
// write to LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *S3Archive) Write(buf []byte) (int, error) {

	if !rf.writing {
		return 0, errors.New("file is not opened for write")
	}

	rf.bytesWritten += int64(len(buf))
	// above counter is not meant to be accurate.
	// so we ignore the cases if actual write below
	// errors out.

	if rf.zw != nil {
		return rf.zw.Write(buf)
	}

	if rf.bw != nil {
		// buffered write: write to the underlying buffer directly
		return rf.bw.Write(buf)
	}

	// else write to the underlying file directly
	return rf.fp.Write(buf)
}

// Read satisfies io.Reader interface - main logic is the transparent
// read from LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *S3Archive) Read(p []byte) (n int, err error) {

	if rf.writing {
		return 0, errors.New("file is not opened for read")
	}

	if rf.zr != nil {
		return rf.zr.Read(p)
	}

	if rf.br != nil {
		// buffered read: read from underlying buffer directly
		return rf.br.Read(p)
	}

	// else read from the underlying file directly
	return rf.fp.Read(p)
}

// Flush complements io.Writer
func (rf *S3Archive) Flush() (err error) {

	if rf.zw != nil {
		err = rf.zw.Flush()
	}

	if err == nil && rf.bw != nil {
		err = rf.bw.Flush()
	}

	if err == nil && rf.fp != nil {
		err = rf.fp.Sync()
	}

	return err
}

// Close works for both Read and Write. Additional
// logic for write to finalize the file from temp-name to
// final-name
func (rf *S3Archive) Close() (err error) {

	err = rf.Flush()
	if err != nil {
		return err
	}

	if rf.zw != nil {
		err = rf.zw.Close()
		rf.zw = nil
	}

	if err != nil {
		return err
	}

	rf.zr = nil

	if rf.fp != nil {
		err = rf.fp.Close()
		rf.fp = nil
	}

	rf.bw = nil
	rf.br = nil

	if rf.writing {
		return rf.finalizeArchiveFile()
	}

	return err
}

func azInit() (err error) {
	gS3Session.Lock() // gS3Session is goroutine safe only after it has been instantiated
	defer gS3Session.Unlock()
	if gS3Session.azPipeline == nil {

		accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
		gS3Session.accountName = accountName
		if len(accountName) == 0 || len(accountKey) == 0 {
			return errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
		}

		// Create a default request pipeline using your storage account name and account key.
		credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return errors.Wrapf(err, "Invalid credentials")
		}

		gS3Session.azPipeline = azblob.NewPipeline(credential,
			azblob.PipelineOptions{Retry: azblob.RetryOptions{TryTimeout: time.Minute * 10}})

	}
	return err
}

// NewArchiveFile creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*S3Archive` returned is an io.Writer
func NewArchiveFile(outDir, prefix, extension string, compress bool, bufferSize int) (rf *S3Archive, err error) {

	err = azInit()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize azure connection")
	}

	// https://play.golang.org/p/vZ4NZzi6vrK
	parts := s3UrlRegex.FindStringSubmatch(outDir)
	if len(parts) != 4 { // must be exactly 4 parts
		return nil, errors.Wrap(err, "Unable to parse azure blob url format")
	}
	containerName, directory := parts[2], parts[3]

	rf = &S3Archive{writing: true,
		outDir:        outDir,
		containerName: containerName,
		directory:     directory,
		prefix:        prefix,
		extension:     extension,
		compress:      compress,
		bufferSize:    bufferSize}

	err = rf.RotateArchiveFile()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// RotateArchiveFile creates a new archive file or if one already exists,
// then it closes the current one and create another empty file
func (rf *S3Archive) RotateArchiveFile() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	if rf.fp != nil {
		err = rf.Close()
		if err != nil {
			return errors.Wrapf(err, "Error closing the current archive file")
		}
	}

	n := time.Now()
	// template time AKA golang magic time is Mon Jan 2 15:04:05 -0700 MST 2006
	ts := n.Format("20060102150405") // playground: http://play.golang.org/p/GRyJmkDevM
	rf.fp, err = ioutil.TempFile("", fmt.Sprintf("%s_%s_", rf.prefix, ts))
	if err != nil {
		return errors.Wrap(err, "Unable to open temporary file for writing")
	}
	rf.name = rf.fp.Name()
	rf.FinalName = fmt.Sprintf("%s/%s.%s", rf.directory, path.Base(rf.name), rf.extension)
	if rf.compress {
		rf.FinalName += ".lz4"
	}
	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", gS3Session.accountName, rf.containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	azContainerURL := azblob.NewContainerURL(*URL, gS3Session.azPipeline)

	var stream io.Writer
	stream = rf.fp
	if rf.bufferSize > 0 {
		log.Printf("Buffer: %d", rf.bufferSize)
		rf.bw = bufio.NewWriterSize(rf.fp, rf.bufferSize)
		stream = rf.bw
	}

	if rf.compress {
		log.Printf("Compression is ON")
		rf.zw = lz4.NewWriter(stream)
	}

	rf.blockBlobURL = azContainerURL.NewBlockBlobURL(rf.FinalName)

	log.Printf("Created file %v", rf.name)
	return err
}

// OpenArchiveFile opens an archive file for reading. `*S3Archive` returned is an io.Reader
func OpenArchiveFile(fileName string, bufferSize int) (rf *S3Archive, err error) {

	err = azInit()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}

	rf = &S3Archive{writing: false}

	rf.fp, err = os.Open(fileName)
	if err != nil {
		log.Printf("Unable to open file %s: +%v", fileName, err)
		return nil, errors.Wrapf(err, "Unable to open file %s", fileName)
	}
	var stream io.Reader
	stream = rf.fp
	if strings.HasSuffix(strings.ToLower(fileName), ".lz4") {
		rf.zr = lz4.NewReader(rf.fp)
		stream = rf.zr
	}
	if bufferSize > 0 {
		rf.br = bufio.NewReaderSize(stream, bufferSize)
	}
	return rf, nil
}

// finalizeArchiveFile is the companion function to CreateArchiveFile().
// finalize will upload to Azure Blobstore.
func (rf *S3Archive) finalizeArchiveFile() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	if rf.bytesWritten == 0 {

		log.Printf("Deleting empty file %s", rf.name)
		err = os.Remove(rf.name)
		return err

	}

	fi, err := os.Stat(rf.name)
	if err != nil {
		err = errors.Wrapf(err, "unable to stat file %s", rf.name)
		return err
	}

	finalFileSize := fi.Size()
	// this will be different from `rf.bytesWritten`
	// because of possible compression

	finalFP, err := os.Open(rf.name)
	if err != nil {
		err = errors.Wrapf(err, "unable to reopen archive file: %s", rf.FinalName)
		return err
	}
	defer finalFP.Close()

	log.Printf("L:%s => AZ:%s [BEGIN]", rf.name, rf.FinalName)

	var statChan = make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go common.UploadProgressPrinter(statChan, rf.name, finalFileSize, &wg)

	_, err = azblob.UploadFileToBlockBlob(context.Background(), finalFP, rf.blockBlobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 4,
		Progress: func(bytesTransferred int64) {
			statChan <- bytesTransferred
		}})
	if err != nil {
		err = errors.Wrapf(err, "ERROR: Blobstore upload error for: %s (%s)", rf.name, rf.FinalName)
		return err
	}
	log.Printf("L:%s => AZ:%s [END]", rf.name, rf.FinalName)

	err = os.Remove(rf.name)
	if err != nil {
		return errors.Wrapf(err, "unable to remove archive file %s after uploading to azure", rf.name)
	}

	close(statChan)
	wg.Wait() // Waiting for status monitor to exit

	rf.bytesWritten = 0 // reset the tracker
	rf.FinalName, rf.name = "", ""

	return nil
}
