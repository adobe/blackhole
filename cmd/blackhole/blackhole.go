/*
Copyright 2020 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in
accordance with the terms of the Adobe license agreement accompanying
it. If you have received this file from a source other than Adobe,
then your use, modification, or distribution of it requires the prior
written permission of Adobe.
*/

package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/adobe/blackhole/lib/archive/file"
	"github.com/adobe/blackhole/lib/archive/request"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// requestConsumer is called as a goroutine, handling
// a single recorder file. To maximize IO, there will be many such recorder threads
func requestConsumer(grID int, rc *runtimeContext) (err error) {

	rf, err := file.NewArchiveFile(rc.outDir, rc.compress, rc.bufferSize)
	if err != nil {
		return errors.Wrapf(err, "Unable to create archive file for worker %d", grID)
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
			if numRequests > numRequestsAtLastSave { // there is something to rotate
				err = rf.RotateArchiveFile()
				numRequestsAtLastSave = numRequests
			}

		case req, more := <-recordReqChan: // Got new request data from bidder?
			if !more {
				break Loop
			}
			numRequests++

			err = rf.SaveRequest(req, false)
			if err != nil {
				msg := fmt.Sprintf("FATAL: Writing to file %s failed.", rf.Name)
				log.Printf("%s: Error: %+v", msg, err)
				return errors.Wrap(err, msg)
			}
		}
	}

	err = rf.Close()
	if err != nil {
		log.Fatalf("#%d# Error closing recorder file: %v", grID, err)
	}

	log.Printf("#%d# Done recording %d requests.", grID, numRequests)
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
