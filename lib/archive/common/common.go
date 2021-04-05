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

// Package common provides functionality to read and write to a archive file
// and keep track of underlying *os.File to close. It also provides a handy
// layer to keep archive files under temporary name until they are closed and
// complete.
package common

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// FinalizerFunc is a callback that will be called on Close.
// Typically one would rename the file to the final desired name
// OR upload the file to S3 or Azure Blobstore. Users of this
// library must provide this call back implementation.
type FinalizerFunc func() (finalName string, err error)

// BasicArchive encapsulates some common functionality between
// S3Archive, FileArchive, and AZArchive
type BasicArchive struct {
	Logger         *zap.Logger
	writing        bool
	deleteOnClose  bool
	fp             *os.File      // Underlying FP. Needed to close and flush after we are done.
	zw             *lz4.Writer   // Used only if compression is enabled.
	zr             *lz4.Reader   // Used only if compression is enabled.
	bw             *bufio.Writer // If set, all writes are buffered
	br             *bufio.Reader // If set, all reads are buffered
	fqfn           string        // name, for debugging/printing only
	stageDir       string
	prefix         string
	extension      string
	compress       bool
	bufferSize     int
	bytesWritten   int64 // to see if file is empty at Close (during finalize)
	Finalizer      FinalizerFunc
	finalizedFiles []string
}

func NewBasicArchive(stageDir, prefix, extension string,
	options ...func(*BasicArchive) error) (ba *BasicArchive, err error) {

	extension = strings.TrimLeft(extension, ".") // allow extension to be specified as xyz or .xyz

	ba = &BasicArchive{
		writing:       true,
		deleteOnClose: false,
		stageDir:      stageDir,
		prefix:        prefix,
		extension:     extension,
	}
	for _, option := range options {
		option(ba)
	}
	if ba.Logger == nil { // still unset, have a default
		ba.Logger, err = zap.NewProduction()
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to initialize zap logger")
		}
	}
	return ba, nil
}

func Compress(c bool) func(*BasicArchive) error {
	return func(b *BasicArchive) error {
		b.compress = c
		return nil
	}
}

func BufferSize(bufferSize int) func(*BasicArchive) error {
	return func(b *BasicArchive) error {
		b.bufferSize = bufferSize
		return nil
	}
}

func Logger(logger *zap.Logger) func(*BasicArchive) error {
	return func(b *BasicArchive) error {
		b.Logger = logger
		return nil
	}
}

func (rf *BasicArchive) Name() string {
	return rf.fqfn
}

func (rf *BasicArchive) TrueContentLength() int64 {
	return rf.bytesWritten
}

// Write satisfies io.Writer interface - main logic is the transparent
// write to LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *BasicArchive) Write(buf []byte) (int, error) {

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
func (rf *BasicArchive) Read(p []byte) (n int, err error) {

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
func (rf *BasicArchive) Flush() (err error) {

	if rf.zw == nil {
		rf.Logger.Debug("Flushing", zap.String("file", rf.Name()))
		if err == nil && rf.bw != nil {
			err = rf.bw.Flush()
		}
		if err == nil && rf.fp != nil {
			err = rf.fp.Sync()
		}
	} else {
		rf.Logger.Warn("Flush() supported only for uncompressed streams")
	}

	return err
}

// Close works for both Read and Write. Additional
// logic for write to finalize the file from temp-name to
// final-name
func (rf *BasicArchive) Close() (err error) {

	if rf.zw != nil {
		err = rf.zw.Close()
		rf.zw = nil
	}

	if err == nil && rf.bw != nil {
		err = rf.bw.Flush()
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

	filePath := rf.Name()
	if (rf.writing && rf.bytesWritten == 0) || (!rf.writing && rf.deleteOnClose) {
		rf.Logger.Debug("Deleting file",
			zap.String("file", filePath),
			zap.Int64("bytesWritten", rf.bytesWritten),
			zap.Bool("deleteOnClose", rf.deleteOnClose))
		err = os.Remove(filePath)
		return err
	}

	if rf.writing && rf.Finalizer != nil {
		var finalName string
		finalName, err = rf.Finalizer()
		if err != nil && finalName != "" {
			rf.finalizedFiles = append(rf.finalizedFiles, finalName)
		}
		return err
	}

	return err
}

func (rf *BasicArchive) FinalizedFiles() []string {
	return rf.finalizedFiles
}

func (rf *BasicArchive) Reset() {
	rf.bytesWritten = 0 // reset the tracker
	rf.fqfn = ""
}

// Rotate creates a new archive file or if one already exists,
// then it closes the current one and create another empty file
// To keep file sizes small, Rotate() must be called at regular
// intervals (either by size or time depending on your preference)
func (rf *BasicArchive) Rotate() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	if rf.fp != nil { // current active file
		err = rf.Close() // Close and finalize file
		if err != nil {
			return errors.Wrapf(err, "Error closing the current archive file")
		}
	}

	n := time.Now()
	// template time AKA golang magic time is Mon Jan 2 15:04:05 -0700 MST 2006
	ts := n.Format("20060102150405") // playground: http://play.golang.org/p/GRyJmkDevM

	extension := ""
	if rf.extension != "" {
		extension += "." + rf.extension
	}
	if rf.compress {
		extension += ".lz4"
	}

	if rf.stageDir != "" {
		err = os.MkdirAll(rf.stageDir, 0755)
		if err != nil {
			return errors.Wrap(err, "Unable to create staging directory for writing")
		}
	}

	rf.fp, err = ioutil.TempFile(rf.stageDir, fmt.Sprintf("%s_%s_*%s.tmp", rf.prefix, ts, extension))
	if err != nil {
		return errors.Wrap(err, "Unable to open temporary file for writing")
	}
	rf.fqfn = rf.fp.Name()

	var stream io.Writer
	stream = rf.fp
	if rf.bufferSize > 0 {
		rf.bw = bufio.NewWriterSize(rf.fp, rf.bufferSize)
		stream = rf.bw
	}

	if rf.compress {
		rf.zw = lz4.NewWriter(stream)
	}

	rf.Logger.Debug("Created", zap.String("file", rf.fqfn), zap.Int("bufferSize", rf.bufferSize), zap.Bool("compress", rf.compress))
	return err
}

// OpenArchive opens an archive file for reading. `*BasicArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int, deleteOnClose bool) (rf *BasicArchive, err error) {

	rf = &BasicArchive{writing: false, deleteOnClose: deleteOnClose}

	rf.fqfn = fileName
	rf.fp, err = os.Open(fileName)
	if err != nil {
		rf.Logger.Error("os.Open failed", zap.String("file", fileName), zap.Error(err))
		return nil, errors.Wrapf(err, "Error opening file %s", fileName)
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

// ProgressPrinter is a helper function. Currently only usable with Azure blob upload
func (rf *BasicArchive) ProgressPrinter(statChan chan int64, fileName string, fileSize int64, wg *sync.WaitGroup) {

	defer wg.Done()
	total := int64(0)
	lastPrinted := int64(0)
	fileName = path.Base(fileName)
	for bytesTransferred := range statChan {
		if bytesTransferred > total {
			diff := bytesTransferred - lastPrinted
			if diff > 100_000_000 {
				if fileSize != 0 {
					rf.Logger.Debug("Upload Status",
						zap.String("file", fileName),
						zap.Int64("bytesTransferred", bytesTransferred),
						zap.Int64("fileSize", fileSize),
						zap.Float64("percentDone", float64(bytesTransferred*100.0)/float64(fileSize)))
				} else {
					rf.Logger.Debug("Upload Status",
						zap.String("file", fileName),
						zap.Int64("bytesTransferred", bytesTransferred))

				}
				lastPrinted = bytesTransferred
			}
		} else {
			rf.Logger.Warn("Previous attempt failed",
				zap.String("file", fileName),
				zap.Int64("Previous bytesTransferred", total),
				zap.Int64("New bytesTransferred", bytesTransferred))
		}
		total = bytesTransferred
	}
	if fileSize != 0 {
		rf.Logger.Debug("Upload Status [FINAL]",
			zap.String("file", fileName),
			zap.Int64("bytesTransferred", total),
			zap.Int64("fileSize", fileSize),
			zap.Float64("percentDone", float64(total*100.0)/float64(fileSize)))
	} else {
		rf.Logger.Debug("Upload Status [FINAL]",
			zap.String("file", fileName),
			zap.Int64("bytesTransferred", total))
	}

}
