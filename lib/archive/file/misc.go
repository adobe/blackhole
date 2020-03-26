/*
Copyright 2020 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in
accordance with the terms of the Adobe license agreement accompanying
it. If you have received this file from a source other than Adobe,
then your use, modification, or distribution of it requires the prior
written permission of Adobe.
*/

package file

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"
)

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
func readFull(r io.Reader, buf []byte, waitForData bool) (bytesRead int, err error) {
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
