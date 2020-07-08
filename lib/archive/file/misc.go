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
