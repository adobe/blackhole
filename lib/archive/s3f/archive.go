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
package s3f

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pierrec/lz4/v3"
	"github.com/pkg/errors"
	"io"
	"log"
	"regexp"
	"sync"
	"time"
	// "github.com/gogo/protobuf/proto"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var gS3Session struct {
	sync.Mutex // Used only for writing. Not for reading
	S3Client   *s3.Client
}

var s3UrlRegex = regexp.MustCompile("([^/:]+)://([^/]+)/(.*?)$")

type S3Archive struct {
	writing      bool
	wg           sync.WaitGroup
	out          *io.PipeWriter // Underlying FP. Needed to close and flush after we are done.
	in           *io.PipeReader // Underlying FP. Needed to close and flush after we are done.
	zw           *lz4.Writer    // Used only if compression is enabled.
	zr           *lz4.Reader    // Used only if compression is enabled.
	bw           *bufio.Writer  // If set, all writes are buffered
	br           *bufio.Reader  // If set, all reads are buffered
	name         string         // name, for debugging/printing only
	outDir       string
	bucketName   string
	directory    string
	prefix       string
	extension    string
	compress     bool
	bufferSize   int
	bytesWritten int64 // to see if file is empty at Close (during finalize)
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
	return rf.out.Write(buf)
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
	return rf.in.Read(p)
}

// Flush complements io.Writer
func (rf *S3Archive) Flush() (err error) {

	if rf.zw != nil {
		err = rf.zw.Flush()
	}

	if err == nil && rf.bw != nil {
		err = rf.bw.Flush()
	}

	// Nothing for raw io.PipeWriter
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

	if rf.out != nil {
		err = rf.out.Close()
		rf.wg.Wait() // wait for s3 writer to finish
		rf.out = nil
	}
	if rf.in != nil {
		err = rf.in.Close()
		rf.in = nil
	}

	rf.bw = nil
	rf.br = nil

	if rf.writing {
		return rf.finalizeArchiveFile()
	}

	return err
}

func s3Init() (err error) {
	gS3Session.Lock() // gS3Session is goroutine safe only after it has been instantiated
	defer gS3Session.Unlock()
	if gS3Session.S3Client == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO())
		if err != nil {
			errors.Wrap(err, "Unable to load default s3 config")
		}
		gS3Session.S3Client = s3.NewFromConfig(cfg)
	}
	return err
}

// NewArchiveFile creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*S3Archive` returned is an io.Writer
func NewArchiveFile(outDir, prefix, extension string, compress bool, bufferSize int) (rf *S3Archive, err error) {

	err = s3Init()
	if err != nil {
		errors.Wrap(err, "Unable to initialize s3 connection")
	}

	// https://play.golang.org/p/vZ4NZzi6vrK
	parts := s3UrlRegex.FindStringSubmatch(outDir)
	if len(parts) != 4 { // must be exactly 4 parts
		errors.Wrap(err, "Unable to parse s3 url format")
	}
	bucketName, directory := parts[2], parts[3]
	rf = &S3Archive{writing: true,
		outDir:     outDir,
		bucketName: bucketName,
		directory:  directory,
		prefix:     prefix,
		extension:  extension,
		compress:   compress,
		bufferSize: bufferSize}

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

	if rf.out != nil {
		err = rf.Close()
		if err != nil {
			return errors.Wrapf(err, "Error closing the current archive file")
		}
	}
	var reader *io.PipeReader
	reader, rf.out = io.Pipe()

	n := time.Now()
	// template time AKA golang magic time is Mon Jan 2 15:04:05 -0700 MST 2006
	ts := n.Format("20060102150405") // playground: http://play.golang.org/p/GRyJmkDevM
	rf.name = fmt.Sprintf("%s/%s_%s_", rf.directory, rf.prefix, ts)
	if rf.compress {
		rf.name += ".lz4"
	}

	rf.wg.Add(1)
	go func(bucket string, filepath string, reader io.Reader, wg *sync.WaitGroup) {
		defer wg.Done()
		gS3Session.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &filepath,
			Body:   reader,
		})
	}(rf.bucketName, rf.name, reader, &rf.wg)

	var stream io.Writer
	stream = rf.out
	if rf.bufferSize > 0 {
		log.Printf("Buffer: %d", rf.bufferSize)
		rf.bw = bufio.NewWriterSize(rf.out, rf.bufferSize)
		stream = rf.bw
	}

	if rf.compress {
		log.Printf("Compression is ON")
		rf.zw = lz4.NewWriter(stream)
	}

	log.Printf("Created file %v", rf.name)
	return err
}

// OpenArchiveFile opens an archive file for reading. `*S3Archive` returned is an io.Reader
func OpenArchiveFile(fileName string, bufferSize int) (rf *S3Archive, err error) {

	err = s3Init()
	if err != nil {
		errors.Wrap(err, "Unable to initialize s3 connection")
	}

	rf = &S3Archive{writing: false}

	/*
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
	*/
	return rf, nil
}

// finalizeArchiveFile is the companion function to CreateArchiveFile().
// finalize will rename the temporary file to its final name. File should be considered
// incomplete if it does not contain `.fbf` or `.fbf.lz4` extension.
// See longer docs there.
func (rf *S3Archive) finalizeArchiveFile() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	/*
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

		fileMode := fi.Mode()
		// only touch the Group and Other sections
		fileMode |= 044

		err = os.Rename(rf.name, rf.FinalName)
		if err != nil {
			err = errors.Wrapf(err, "unable to rename archive file: %s", rf.name)
			return err
		}
		log.Printf("Renamed %s to %s", rf.name, rf.FinalName)

		err = os.Chmod(rf.FinalName, fileMode)
		if err != nil {
			err = errors.Wrapf(err, "unable to chmod archive file: %s", rf.FinalName)
			return err
		}

		rf.bytesWritten = 0 // reset the tracker
	*/

	return nil
}