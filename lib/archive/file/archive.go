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
package file

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// FileArchive embeds a BasicArchive with some additional attributes
type FileArchive struct {
	common.BasicArchive
}

// NewArchive creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*FileArchive` returned is an io.Writer
func NewArchive(outDir, prefix, extension string, options ...func(*common.BasicArchive) error) (rf *FileArchive, err error) {

	outDir = strings.TrimPrefix(outDir, "file://")

	ba, err := common.NewBasicArchive(
		outDir, prefix, extension, options...)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to initialize basic archive")
	}

	rf = &FileArchive{BasicArchive: *ba}
	rf.Finalizer = rf.finalizeArchive

	err = rf.Rotate()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// OpenArchive opens an archive file for reading. `*FileArchive` returned is an io.Reader
func OpenArchive(fileName string, bufferSize int) (rf *FileArchive, err error) {

	rfi, err := common.OpenArchive(fileName, bufferSize, false)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize s3 connection")
	}
	return &FileArchive{BasicArchive: *rfi}, nil
}

// finalizeArchive is the companion function to CreateArchiveFile().
// finalize will rename the temporary file to its final name. File should be considered
// incomplete if it ends with a .tmp extension.
func (rf *FileArchive) finalizeArchive() (finalFile string, err error) {

	filePath := rf.Name()
	fi, err := os.Stat(filePath)
	if err != nil {
		err = errors.Wrapf(err, "unable to stat file %s", filePath)
		return "", err
	}

	finalPath := strings.TrimSuffix(filePath, ".tmp")
	err = os.Rename(filePath, finalPath)
	if err != nil {
		err = errors.Wrapf(err, "unable to rename archive file: %s", filePath)
		return "", err
	}
	finalFile = path.Base(finalPath) // Only filename part

	rf.Logger.Info("Renamed",
		zap.String("old", filePath),
		zap.String("new", finalPath),
		zap.Int64("content-bytes", rf.TrueContentLength()),
		zap.Int64("compressed-bytes", fi.Size()))
	fileMode := fi.Mode()
	// only touch the Group and Other sections
	fileMode |= 044

	err = os.Chmod(finalPath, fileMode)
	if err != nil {
		err = errors.Wrapf(err, "unable to chmod archive file: %s", finalPath)
		return "", err
	}

	return finalFile, nil
}

func List(dir string) (files []string, err error) {

	err = filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("ERROR: %s: %+v\n", path, err)
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return nil // Skipping weird directories
		}
		if !info.IsDir() {
			files = append(files, relPath)
		}

		/* this is better code to include sub-directories
		 * but they are a bit more work to support.
		 *
		 * For now, we don't need to list or remove sub-directories

		// fmt.Printf("path = >%s<, relpath=>%s<\n", path, relPath)
		if relPath != "" && relPath != "." {
			// add all subdirectories except the root directory
			files = append(files, relPath)
		}
		*/
		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "list directory failed for: %s", dir)
	}
	return files, err
}

func Delete(dir string, files []string) (err error) {
	for _, file := range files {
		err = os.Remove(path.Join(dir, file))
		if err != nil {
			fmt.Printf("ERROR: Can't delete %s: %+v", file, err)
			return nil
		}
	}
	return err
}
