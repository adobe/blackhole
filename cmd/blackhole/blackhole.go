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
	"sync/atomic"
	"time"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/adobe/blackhole/lib/archive/common"
	"go.uber.org/zap"

	"github.com/adobe/blackhole/lib/request"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
)

// requestConsumer is called as a goroutine, handling
// a single recorder file. To maximize IO, there will be many such recorder threads
// when dummy = true, the worker just counts requests
func requestConsumer(grID int, rc *runtimeContext, dummy bool) (err error) {

	llg := rc.logger.With(zap.Int("thread", grID))

	var rf archive.Archive
	if !dummy {
		rf, err = archive.NewArchive(rc.outDir,
			"requests", ".fbf",
			common.Compress(true),
			common.BufferSize(rc.bufferSize),
			common.Logger(rc.logger))
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
			llg.Warn("Got exit signal", zap.Int("thread-id", grID))
			break Loop

		case <-tickerPrint.C:
			atomic.StoreInt64(&rc.counters[grID], int64(numRequests))
			llg.Debug("Got requests",
				zap.Int("requests", numRequests))

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
					llg.Error("Write failed",
						zap.String("file", rf.Name()))
					return errors.Wrap(err, msg)
				}
			}
		}
	}

	if !dummy {
		err = rf.Close()
		if err != nil {
			msg := fmt.Sprintf("FATAL: closing file %s failed.", rf.Name())
			llg.Error("Closing failed",
				zap.String("file", rf.Name()))
			return errors.Wrap(err, msg)
		}
		llg.Debug("Done",
			zap.Int("requests", numRequests))
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
