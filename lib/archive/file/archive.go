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
package file

import (
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/pkg/errors"
	"log"
	"os"
	"strings"
)

// FileArchive embeds a BasicArchive with some additional attributes
type FileArchive struct {
	common.BasicArchive
}

// NewArchive creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*FileArchive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, compress bool, bufferSize int) (rf *FileArchive, err error) {

	rf = &FileArchive{BasicArchive: *common.NewBasicArchive(outDir,
		prefix, extension, compress, bufferSize,
		rf.finalizeArchive)}

	err = rf.Rotate()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// OpenArchive opens an archive file for reading. `*FileArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *FileArchive, err error) {

	rfi, err := common.OpenArchive(fileName, bufferSize)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}
	return &FileArchive{BasicArchive: *rfi}, nil
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will rename the temporary file to its final name. File should be considered
// incomplete if it ends with a .tmp extension.
func (rf *FileArchive) finalizeArchive(filepath string, fileSize int64) (err error) {

	finalPath := filepath
	if strings.HasSuffix(filepath, ".tmp") {
		finalPath = finalPath[:(len(".tmp"))]
		err = os.Rename(filepath, finalPath)
		if err != nil {
			err = errors.Wrapf(err, "unable to rename archive file: %s", filepath)
			return err
		}
	}
	log.Printf("Renamed %s to %s", filepath, finalPath)

	fi, err := os.Stat(filepath)
	if err != nil {
		err = errors.Wrapf(err, "unable to stat file %s", filepath)
		return err
	}

	fileMode := fi.Mode()
	// only touch the Group and Other sections
	fileMode |= 044

	err = os.Chmod(finalPath, fileMode)
	if err != nil {
		err = errors.Wrapf(err, "unable to chmod archive file: %s", finalPath)
		return err
	}

	rf.Reset()
	return nil
}
