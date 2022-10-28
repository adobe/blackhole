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

// Package file provides archive interface for local directories/files
package sum

import (
	"errors"
	"fmt"
	"hash"

	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/cespare/xxhash"
)

/*
type Archive interface {
	io.ReadWriteCloser
	Rotate() (err error)
	Flush() (err error)
	Name() string
	FinalizedFiles() ([]string, map[string]common.ArchiveFileStats)
}
*/

// ChecksumArchive embeds a BasicArchive with some additional attributes
type ChecksumArchive struct {
	xh               hash.Hash64
	bytesWritten     int64
	chunksWritten    int64
	finalizedFiles   []string
	finalizedDetails map[string]common.ArchiveFileDetails
}

// NewArchive creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*ChecksumArchive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, options ...func(*common.BasicArchive) error) (rf *ChecksumArchive, err error) {

	xh := xxhash.New()

	rf = &ChecksumArchive{xh: xh}
	rf.finalizedDetails = make(map[string]common.ArchiveFileDetails)

	return rf, err
}

// OpenArchive opens an archive file for reading. `*FileArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *ChecksumArchive, err error) {

	panic("Not implemented")
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will rename the temporary file to its final name. File should be considered
// incomplete if it ends with a .tmp extension.
func (rf *ChecksumArchive) finalizeArchive() (finalFile string, err error) {

	finalFile = fmt.Sprintf("%0X", rf.xh.Sum64())
	return finalFile, nil
}

func List(dir string) (files []string, err error) {
	panic("Not implemented")
}

func Delete(dir string, files []string) (err error) {
	panic("Not implemented")
}

// Write satisfies io.Writer interface - main logic is the transparent
// write to LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *ChecksumArchive) Write(buf []byte) (int, error) {

	rf.bytesWritten += int64(len(buf))
	rf.chunksWritten += 1
	return rf.xh.Write(buf)
}

// Read satisfies io.Reader interface - main logic is the transparent
// read from LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *ChecksumArchive) Read(p []byte) (n int, err error) {

	return 0, errors.New("Read not supported for checksum target")
}

// Flush complements io.Writer
func (rf *ChecksumArchive) Flush() (err error) {
	return nil
}

// Close works for both Read and Write. Additional
// logic for write to finalize the file from temp-name to
// final-name
func (rf *ChecksumArchive) Close() (err error) {

	fileName, err := rf.finalizeArchive()
	if err != nil {
		return err
	}
	rf.finalizedFiles = append(rf.finalizedFiles, fileName)
	rf.finalizedDetails[fileName] = common.ArchiveFileDetails{
		FileName:      "", // Filename is purposely kept empty - there is no real archive file created
		BytesWritten:  rf.bytesWritten,
		ChunksWritten: rf.chunksWritten,
		Checksum:      fileName}

	return nil
}

func (rf *ChecksumArchive) FinalizedFiles() map[string]common.ArchiveFileDetails {

	return rf.finalizedDetails
}

func (rf *ChecksumArchive) Name() string {
	return "placeholder.checksum"
}

func (rf *ChecksumArchive) Rotate() (err error) {
	return nil
}
