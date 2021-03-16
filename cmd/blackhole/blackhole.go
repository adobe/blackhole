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

package main

import (
	"fmt"
	"github.com/adobe/blackhole/lib/archive"
	"log"
	"sync/atomic"
	"time"

	"github.com/adobe/blackhole/lib/request"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// requestConsumer is called as a goroutine, handling
// a single recorder file. To maximize IO, there will be many such recorder threads
// when dummy = true, the worker just counts requests
func requestConsumer(grID int, rc *runtimeContext, dummy bool) (err error) {

	var rf archive.Archive
	if !dummy {
		rf, err = archive.NewArchive(rc.outDir,
			"requests", ".fbf", rc.compress, rc.bufferSize)
		if err != nil {
			return errors.Wrapf(err, "Unable to create archive file for worker %d", grID)
		}
	}

	numRequests := 0
	numRequestsAtLastSave := 0

	tickerPrint := time.NewTicker(5 * time.Second) // Flush at least once in 5 seconds
	defer tickerPrint.Stop()

	tickerSave := time.NewTicker(10 * time.Minute) // Flush at least once in 5 seconds
	defer tickerSave.Stop()

Loop:
	for {

		select {

		case <-rc.exitChans[grID]:
			log.Printf("Worker %d got exit signal", grID)
			break Loop

		case <-tickerPrint.C:
			atomic.StoreInt64(&rc.counters[grID], int64(numRequests))
			log.Printf("#%d# Received %d requests", grID, numRequests)

		case <-tickerSave.C:
			if !dummy && numRequests > numRequestsAtLastSave { // there is something to rotate
				err = rf.Rotate()
				numRequestsAtLastSave = numRequests
			}

		case req, more := <-recordReqChan: // Got new request data from bidder?
			if !more {
				break Loop
			}
			numRequests++
			if !dummy {
				err = req.SaveRequest(rf, false)
				if err != nil {
					msg := fmt.Sprintf("FATAL: writing to file %s failed.", rf.Name())
					log.Printf("%s: Error: %+v", msg, err)
					return errors.Wrap(err, msg)
				}
			}
		}
	}

	if !dummy {
		err = rf.Close()
		if err != nil {
			log.Fatalf("#%d# Error closing recorder file: %v", grID, err)
		}
		log.Printf("#%d# Done recording %d requests.", grID, numRequests)
	}

	rc.wgConsumers.Done()

	return err
}

// fastHTTPHandler is the request handler in fasthttp style, i.e. just plain function.
func fastHTTPHandler(ctx *fasthttp.RequestCtx) {

	if recordReqChan != nil { // MARK-b5688e1019ad (see this code elsewhere)
		ar := request.CreateRequestFromFastHTTPCtx(ctx)
		recordReqChan <- ar
	}
}
