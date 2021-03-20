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

/*
sender provides async http(s) egress functionality over a channel.
The code follows a
 [driver] -> [channel] -> [worker(s)]
pattern that is common in Go. Requests are sent over a channel and response
body is currently ignored. Each worker will wait for http request to finish.
Caller is expected to create multiple worker independently for throughput.
One worker will only have one outstanding http request. Parallelism is controlled
entirely by how many workers are created by the caller.
TODO: track and record request status by X-Request-ID
*/
package sender

import (
	"bufio"
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/adobe/blackhole/lib/fbr"
	"github.com/adobe/blackhole/lib/request"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type Worker struct {
	targetHost    string
	bufPoolHeader sync.Pool
	bufPoolURLs   sync.Pool
	reqChan       chan *request.UnmarshalledRequest
	errorRespChan chan bool
	grID          int // like a thread-id
	wg            *sync.WaitGroup
	logger        *zap.Logger

	// optional items
	dryRun           bool
	extract2file     bool
	reqID            string
	quiet            bool
	minDelayMs       int
	exitOnFirstError bool
	outputDir        string
}

// Option controlls a set of options that can be set on Worker
// This is to make it convenient to control settings without the
// New method taking tens of arguments
type Option func(*Worker)

// NewWorker creates a new instance of an 'http egress worker' with a few datastructures
// it keeps track of. Please note that NewWorker does not start a thread or go-routine,
// caller must call `go worker.Run()` when all options have been set and it ready to start
// the job. Job details are consumed over a channel, `reqChan`, created by the caller
// and a given worker will process an unlimited number of tasks. Caller must close the `reqChan`
// to indicate end of job. Caller must also listen to (and consume) `errorRespChan` for errors
// If caller does not consume errorRespChan, deadlocks will occur.
func NewWorker(reqChan chan *request.UnmarshalledRequest, errorRespChan chan bool, targetHost string, wg *sync.WaitGroup, grID int) (wrk *Worker) {
	wrk = &Worker{
		bufPoolHeader: sync.Pool{
			New: func() interface{} {
				// The Pool's New function should generally only return pointer
				// types, since a pointer can be put into the return interface
				// value without an allocation:
				v := make([]byte, 4096)
				return v
			},
		},
		bufPoolURLs: sync.Pool{
			New: func() interface{} {
				// The Pool's New function should generally only return pointer
				// types, since a pointer can be put into the return interface
				// value without an allocation:
				v := make([]byte, 1024)
				return v
			},
		},
		reqChan:       reqChan,
		errorRespChan: errorRespChan,
		targetHost:    targetHost,
		wg:            wg,
		grID:          grID,
	}
	wrk.logger, _ = zap.NewDevelopment()
	// defaults to verbose because of our reverse-meaning argument `quiet`
	return wrk
}

func (wrk *Worker) WithOption(opts ...Option) {
	for _, opt := range opts {
		opt(wrk)
	}
}

func Quiet(quiet bool) Option {
	return func(wrk *Worker) {
		wrk.quiet = quiet
		if quiet {
			wrk.logger, _ = zap.NewProduction()
		}
	}
}

func ExtractToFile(extractToFile bool) Option {
	return func(wrk *Worker) {
		wrk.extract2file = extractToFile
	}
}
func Dryrun(dryRun bool) Option {
	return func(wrk *Worker) {
		wrk.dryRun = dryRun
	}
}
func MatchReqID(reqID string) Option {
	return func(wrk *Worker) {
		wrk.reqID = reqID
	}
}
func MinDelayMS(minDelayMs int) Option {
	return func(wrk *Worker) {
		wrk.minDelayMs = minDelayMs
	}
}
func ExitOnFirstError(exitOnFirstError bool) Option {
	return func(wrk *Worker) {
		wrk.exitOnFirstError = exitOnFirstError
	}
}

func OutputDirectory(outputDir string) Option {
	return func(wrk *Worker) {
		wrk.outputDir = outputDir
	}
}

func (wrk *Worker) replayRequest(reqEnvelope *fbr.Request) (err error) {

	if !wrk.dryRun {
		urlb := wrk.bufPoolURLs.Get().([]byte)
		defer wrk.bufPoolURLs.Put(urlb)
		// -----------------------------------------------------------------------
		// following is an optimized version of
		// fmt.Sprintf("http://%s%s", args.targetHostPort, reqEnvelope.Uri())
		// -----------------------------------------------------------------------
		urlb = append(urlb[:0], []byte("http://")...)
		urlb = append(urlb, wrk.targetHost...)
		urlb = append(urlb, reqEnvelope.Uri()...)

		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)

		headerb := wrk.bufPoolHeader.Get().([]byte)
		defer wrk.bufPoolHeader.Put(headerb)
		// -----------------------------------------------------------------------
		// Manually create HTTP Request-Line. This is not part of original request
		// Request-Line   = Method SP Request-URI SP HTTP-Version CRLF
		// following is an optimized version of
		// fmt.Sprintf("%s %s HTTP/1.1\n",
		//		reqEnvelope.Method(), reqEnvelope.Uri(), reqEnvelope.Headers())
		// -----------------------------------------------------------------------
		headerb = append(headerb[:0], reqEnvelope.Method()...)
		headerb = append(headerb, ' ')
		headerb = append(headerb, reqEnvelope.Uri()...)
		headerb = append(headerb, []byte(" HTTP/1.1\n")...)
		headerb = append(headerb, reqEnvelope.Headers()...)
		err = req.Header.Read(bufio.NewReader(bytes.NewReader(headerb)))
		if err != nil {
			return errors.Wrap(err, "Failed to assemble header for outgoing request")
		}
		req.SetRequestURIBytes(urlb)

		req.SetBody(reqEnvelope.BodyBytes())
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		err = fasthttp.Do(req, resp)
		if err != nil {
			return errors.Wrap(err, "Proxy request failed")
		}
		if resp.StatusCode() != http.StatusOK {
			return errors.Wrap(err, "Proxy request errored")
		}
		_ = resp.Body()
	} else if wrk.extract2file {
		err = wrk.saveRequest(reqEnvelope, false)
		if err != nil {
			return errors.Wrapf(err, "Got an error trying to save request to file")
		}
	} else {
		err = wrk.saveRequest(reqEnvelope, true)
		if err != nil {
			return errors.Wrapf(err, "Got an error trying to save request to file")
		}
	}

	return nil
}

func (wrk *Worker) processAndRelease(umr *request.UnmarshalledRequest) (curReqID string, err error) {

	req := umr.Request()
	curReqID = ""
	defer umr.Release()

	if wrk.reqID == "" || bytes.Equal(req.Id(), []byte(wrk.reqID)) {

		if !wrk.quiet {
			wrk.logger.Debug("Request",
				zap.String("Request-ID", curReqID),
				zap.ByteString("URL", req.Uri()))
		}
		err = wrk.replayRequest(req)
		if err != nil {
			return curReqID, errors.Wrap(err, "Request replay failed")
		}

	} else if !wrk.quiet {
		wrk.logger.Debug("Skipping",
			zap.String("Request-ID", curReqID),
			zap.ByteString("URL", req.Uri()))
	}

	return curReqID, nil
}

func (wrk *Worker) Run() {

	expectedDelay := time.Millisecond * time.Duration(wrk.minDelayMs)
	warnedUserAlready := false

	var err error
	var curReqID string

Loop:
	for req := range wrk.reqChan {

		st := time.Now()
		curReqID, err = wrk.processAndRelease(req)
		if err != nil {
			wrk.logger.Error("Unexpected response from server",
				zap.Error(err))
			if wrk.exitOnFirstError {
				wrk.errorRespChan <- true
				break Loop
			} else {
				continue
			}
		} else if wrk.minDelayMs > 0 {
			actDelay := time.Since(st)
			if actDelay < expectedDelay {
				time.Sleep(expectedDelay - actDelay)
			} else if !warnedUserAlready && actDelay > expectedDelay*2 {
				warnedUserAlready = true
				wrk.logger.Warn("Actual delay is way bigger than minimum delay",
					zap.Duration("actual", actDelay),
					zap.Duration("expected", expectedDelay))
			}
		}

		if wrk.reqID != "" && curReqID == wrk.reqID {
			wrk.errorRespChan <- true
			break Loop
		}
	}
	wrk.wg.Done()

}
