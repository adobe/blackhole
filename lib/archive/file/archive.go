/*
Copyright 2020 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in
accordance with the terms of the Adobe license agreement accompanying
it. If you have received this file from a source other than Adobe,
then your use, modification, or distribution of it requires the prior
written permission of Adobe.
*/

// Package file provides functionality to read and write to a archive file of
// HTTP requests. Requests are serialized using Flatbuffers (See request.fbs for schema)
// Optionally data can be compressed using LZ4. LZ4 provides a CPU friendly compression
package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/adobe/blackhole/lib/archive/request"
	"github.com/pierrec/lz4/v3"
	"github.com/pkg/errors"
	// "github.com/gogo/protobuf/proto"
)

type Archive struct {
	writing      bool
	fp           *os.File      // Underlyinf FP. Needed to close and flush after we are done.
	zw           *lz4.Writer   // Only if compression is enabled.
	zr           *lz4.Reader   // Only if compression is enabled.
	bw           *bufio.Writer // All writes are buffered
	br           *bufio.Reader // All writes are buffered
	Name         string        // name, for debugging/printing only
	FinalName    string
	outDir       string
	compress     bool
	bufferSize   int
	bytesWritten int64 // to see if file is empty at Close (during finalize)
}

// Write satisfies io.Writer interface - main logic is the transparent
// write to LZ4, Bufio, or Raw FP depending on how the file was
// opened
func (rf *Archive) Write(buf []byte) (int, error) {

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
func (rf *Archive) Read(p []byte) (n int, err error) {

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
	return rf.br.Read(p)
}

// Flush complements io.Writer
func (rf *Archive) Flush() (err error) {

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
func (rf *Archive) Close() (err error) {

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

// SaveRequest saves the data held by MarshalledRequest to the archive file.
// MarshalledRequest typically holds a flatbuffer builder that is already
func (rf *Archive) SaveRequest(req *request.MarshalledRequest, flushNow bool) (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	const UINT64LEN = 8
	var lbuf = make([]byte, UINT64LEN)

	fbBytes := req.Bytes()
	fbLen := len(fbBytes)

	defer req.Release()

	binary.LittleEndian.PutUint64(lbuf, uint64(fbLen))

	n, err := rf.Write(lbuf)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, UINT64LEN)
		log.Printf("%s: Error: %+v", msg, err)
		return errors.Wrap(err, msg)
	}

	n, err = rf.Write(fbBytes)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, fbLen)
		log.Printf("%s: Error: %+v", msg, err)
		return errors.Wrap(err, msg)
	}

	if flushNow {
		return rf.Flush()
	}
	return err
}

// GetNextRequest reads an archived request from a stream (io.Reader), allocates
// a buffer from a Pool and returns a lease to that buffer. Caller must do the following
//
// 	1. Call umr, err := GetNextRequest() for the next item.
//	2. Handle io.EOF properly as not an error, but end of processing.
// 	3. req := umr.Request() <- Use the flatbuff generated types and code
// 	4. Call umr.Release() with the original slice to free
//
// Why do we return a `*UnmarshalledRequest` instead of a `fbr.Request` object?
//
// Buffer pool needs to be managed at the level of `UnmarshalledRequest` to save allocations
// If `fbr.Request` is returned instead of the wrapper object, it may not work. Caller can't
// use a pointer to `fbr.Request` and return the original buffer to the pool. More research
// is needed to identity if the intermediate object `*UnmarshalledRequest` can be removed.
// Anyways this intermediate object does not cause inefficiency,
// except for the wierdness in the API
func (rf *Archive) GetNextRequest(waitForData bool) (umr *request.UnmarshalledRequest, err error) {

	umr = request.CreateUMRequest()
	const UINT64LEN = 8

	sizeBuf := make([]byte, UINT64LEN)
	_, err = readFull(rf, sizeBuf, waitForData)
	if err != nil {
		if err == io.EOF {
			umr.Release()
			return nil, io.EOF
		}
		umr.Release()
		return nil, err
	}

	fbLen := int(binary.LittleEndian.Uint64(sizeBuf))
	umr.Grow(fbLen)
	n, err := readFull(rf, umr.Bytes(), waitForData)
	if err != nil {
		umr.Release()
		msg := fmt.Sprintf("FATAL: Read only %d bytes, %d expected.", n, fbLen)
		log.Printf("%s: Error: %+v", msg, err)
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // at this stage simple end-of-file is not allowed
		}
		err = errors.Wrapf(err, msg) // No longer just an end-of-file "EOF" error
		return nil, err
	}

	// req = archive.GetRootAsRequest(lease[:fbLen], 0)
	return umr, nil
}

// NewArchiveFile creates a new recorder file (for writing). The caller must call
// `rf.Close()` on the resulting handle to close out the file.
// File is atomically renamed to the final name only after everything
// is flushed to disk and file is closed. `*Archive` returned is an io.Writer
func NewArchiveFile(outDir string, compress bool, bufferSize int) (rf *Archive, err error) {

	rf = &Archive{writing: true, outDir: outDir, compress: compress, bufferSize: bufferSize}

	err = rf.RotateArchiveFile()
	if err != nil {
		return nil, err
	}
	return rf, err
}

// RotateArchiveFile creates a new archive file or if one already exists,
// then it closes the current one and create another empty file
func (rf *Archive) RotateArchiveFile() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	if rf.fp != nil {
		rf.Close()
	}
	fqdn := path.Join(rf.outDir, "requests")
	err = os.MkdirAll(fqdn, 0755)
	if err != nil {
		return errors.Wrapf(err, "Unable to create temporary directory")
	}
	n := time.Now()
	// template time AKA golang magic time is Mon Jan 2 15:04:05 -0700 MST 2006
	ts := n.Format("20060102150405") // playground: http://play.golang.org/p/GRyJmkDevM
	rf.fp, err = ioutil.TempFile(fqdn, "requests_"+ts+"_")
	if err != nil {
		return errors.Wrapf(err, "Unable to open temporary file")
	}
	rf.Name = rf.fp.Name()
	rf.FinalName = rf.Name + ".fbf"

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
		rf.FinalName += ".lz4"
	}

	log.Printf("Created file %v", rf.Name)
	return err
}

// OpenArchiveFile opens an archive file for reading. `*Archive` returned is an io.Reader
func OpenArchiveFile(fileName string, bufferSize int) (rf *Archive, err error) {

	rf = &Archive{writing: false}

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
// finalize will rename the temporary file to its final name. File should be considered
// incomplete if it does not contain `.fbf` or `.fbf.lz4` extension.
// See longer docs there.
func (rf *Archive) finalizeArchiveFile() (err error) {

	if !rf.writing {
		return errors.New("file is not opened for write")
	}

	if rf.bytesWritten == 0 {

		log.Printf("Deleting empty file %s", rf.Name)
		os.Remove(rf.Name)
		return err

	}

	fi, err := os.Stat(rf.Name)
	if err != nil {
		err = errors.Wrapf(err, "unable to stat file %s", rf.Name)
		return err
	}

	fileMode := fi.Mode()
	// only touch the Group and Other sections
	fileMode |= 044

	err = os.Rename(rf.Name, rf.FinalName)
	if err != nil {
		err = errors.Wrapf(err, "unable to rename archive file: %s", rf.Name)
		return err
	}
	log.Printf("Renamed %s to %s", rf.Name, rf.FinalName)

	err = os.Chmod(rf.FinalName, fileMode)
	if err != nil {
		err = errors.Wrapf(err, "unable to chmod archive file: %s", rf.FinalName)
		return err
	}

	rf.bytesWritten = 0 // reset the tracker

	return nil
}
