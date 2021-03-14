package request

import (
	"encoding/binary"
	"fmt"
	"github.com/adobe/blackhole/lib/archive"
	"github.com/pkg/errors"
	"io"
	"log"
	"time"
)

// SaveRequest saves the data held by MarshalledRequest to the archive file.
// MarshalledRequest typically holds a flatbuffer builder that is already
func (req *MarshalledRequest) SaveRequest(rf archive.Archive, flushNow bool) (err error) {

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
func GetNextRequest(rf archive.Archive, waitForData bool) (umr *UnmarshalledRequest, err error) {

	umr = CreateUMRequest()
	const UINT64LEN = 8

	sizeBuf := make([]byte, UINT64LEN)
	_, err = ReadFull(rf, sizeBuf, waitForData)
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
	n, err := ReadFull(rf, umr.Bytes(), waitForData)
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

// readFull is similar in functionality and uses stdlib's io.ReadFull
// (in that in tries to fill the buffer) but when an EOF is encountered
// it can optionally sleep & wait for data (if `waitForData` is true)
//
// Think of the behavior akin to unix `tail -f`.
// This behavior only applies when `waitForData=true`
// waitForData is to allow reading from a file that has not yet been flushed completely
// from another process or thread.
//
// `waitForData` is to be used in special circumstances when archive file is concurrently being
// filled from another process. WARNING: If the underlying the io.Reader is decompressor,
// it may misbehave in this kind of usage. Care must be taken to use this option only with
// plain text archive files.
func ReadFull(r io.Reader, buf []byte, waitForData bool) (bytesRead int, err error) {
	tries := 0
	for bytesRead < len(buf) {
		n, err := io.ReadFull(r, buf[bytesRead:])
		bytesRead += n
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if waitForData && tries < 600 {
					tries++
					// log.Printf("Consumer: No data to read: sleeping one second (tries = %d)", tries)
					time.Sleep(time.Second)
					continue
				}
				return bytesRead, err
			}
			msg := fmt.Sprintf("FATAL: Read only %d bytes, %d expected.", n, len(buf))
			log.Printf("%s: Error: %+v", msg, err)
			return bytesRead, errors.Wrap(err, msg)
		}
		tries = 0
	}

	return bytesRead, err
}
