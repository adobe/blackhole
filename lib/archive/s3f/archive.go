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
package s3f

import (
	"context"
	"fmt"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
)

var gS3Session struct {
	sync.Mutex   // Used only for writing. Not for reading
	S3Client     *s3.Client
	S3Uploader   *manager.Uploader
	S3Downloader *manager.Downloader
}

var s3UrlRegex = regexp.MustCompile("([^/:]+)://([^/]+)/(.*?)$")

type S3Archive struct {
	common.BasicArchive
	bucketName string
	s3SubDir   string
}

func s3Init() (err error) {
	gS3Session.Lock() // gS3Session is goroutine safe only after it has been instantiated
	defer gS3Session.Unlock()
	if gS3Session.S3Client == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			return errors.Wrap(err, "Unable to load default s3 config")
		}
		gS3Session.S3Client = s3.NewFromConfig(cfg)
		gS3Session.S3Uploader = manager.NewUploader(gS3Session.S3Client)
		gS3Session.S3Downloader = manager.NewDownloader(gS3Session.S3Client)
	}
	return err
}

// NewArchive creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*S3Archive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, compress bool, bufferSize int) (rf *S3Archive, err error) {

	err = s3Init()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}

	// https://play.golang.org/p/vZ4NZzi6vrK
	parts := s3UrlRegex.FindStringSubmatch(outDir)
	if len(parts) != 4 { // must be exactly 4 parts
		return nil, errors.Wrap(err, "Unable to parse s3 url format")
	}
	bucketName, s3SubDir := parts[2], parts[3]
	rf = &S3Archive{BasicArchive: *common.NewBasicArchive("",
		prefix, extension, compress, bufferSize),
		bucketName: bucketName,
		s3SubDir:   s3SubDir}
	rf.Finalizer = rf.finalizeArchive

	err = rf.Rotate()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// OpenArchive opens an archive file for reading. `*S3Archive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *S3Archive, err error) {

	err = s3Init()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}

	parts := s3UrlRegex.FindStringSubmatch(fileName)
	if len(parts) != 4 { // must be exactly 4 parts
		return nil, errors.Wrap(err, "Unable to parse azure blob url format")
	}

	bucketName, filePath := parts[2], parts[3]

	fp, err := ioutil.TempFile("", fmt.Sprintf("tmp_*_%s", path.Base(filePath)))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create temp file")
	}

	log.Printf("AZ:%s => L:%s [BEGIN]", filePath, fp.Name())
	_, err = gS3Session.S3Downloader.Download(context.Background(), fp, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &filePath,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to download archive file: %s", filePath)
	}
	log.Printf("AZ:%s => L:%s [END]", filePath, fp.Name())

	rfi, err := common.OpenArchive(fp.Name(), bufferSize, true)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}
	return &S3Archive{BasicArchive: *rfi}, nil
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will upload to S3.
func (rf *S3Archive) finalizeArchive() (err error) {

	filePath := rf.Name()
	finalPath := fmt.Sprintf("%s/%s", rf.s3SubDir, path.Base(filePath))
	finalPath = strings.TrimSuffix(finalPath, ".tmp")

	finalFP, err := os.Open(filePath)
	if err != nil {
		return errors.Wrapf(err, "unable to reopen archive file: %s", filePath)
	}
	defer finalFP.Close()

	log.Printf("L:%s => AZ:%s [BEGIN]", filePath, finalPath)

	/* Progress tracking is not possible with S3manager API
	 * suggested work around is hacky
	 * https://github.com/aws/aws-sdk-go/pull/1868#issuecomment-514097090

	fi, err := os.Stat(rf.name)
	if err != nil {
		return errors.Wrapf(err, "unable to stat file %s", rf.name)
	}

	finalFileSize := fi.Size()
	// this will be different from `rf.bytesWritten`
	// because of possible compression

	var statChan = make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go common.UploadProgressPrinter(statChan, rf.name, finalFileSize, &wg)
	*/

	_, err = gS3Session.S3Uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: &rf.bucketName,
		Key:    &finalPath,
		Body:   finalFP,
	})
	if err != nil {
		return errors.Wrapf(err, "unable to reopen archive file: %s", finalPath)
	}
	log.Printf("L:%s => AZ:%s [END]", filePath, finalPath)

	err = os.Remove(filePath)
	if err != nil {
		return errors.Wrapf(err, "unable to remove archive file %s after uploading to azure", filePath)
	}
	rf.Reset()

	/* Progress tracking is not possible with S3manager API
	 * suggested work around is hacky
	 * https://github.com/aws/aws-sdk-go/pull/1868#issuecomment-514097090

	close(statChan)
	wg.Wait() // Waiting for status monitor to exit

	*/

	return nil
}
