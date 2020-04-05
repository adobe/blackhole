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
	"io"
	"log"
	"sync"

	"github.com/adobe/blackhole/lib/archive/file"
	"github.com/adobe/blackhole/lib/archive/request"
	"github.com/adobe/blackhole/lib/sender"
	"github.com/pkg/errors"
)

// replayFile replays a given file
func replayFile(fileName string, args *cmdArgs) (err error) {

	const archiveFileReadBufSize = 65536 // 64 K
	var numRequestsMade = 0

	rf, err := file.OpenArchiveFile(fileName, archiveFileReadBufSize)
	if err != nil {
		return errors.Wrapf(err, "Unable to open archive file: %s", fileName)
	}
	defer rf.Close()

	reqChan := make(chan *request.UnmarshalledRequest)

	var wg sync.WaitGroup
	errorRespChan := make(chan bool, args.numReqThreads)
	// Buffer of `errorRespChan` must match worker count. Every worker must be
	// able to write an error an exit (without blocking) even if
	// main already bailed out of the select after getting an
	// error from another worker.

	for i := 0; i < args.numReqThreads; i++ {
		wrk := sender.NewWorker(reqChan, errorRespChan, args.targetHost, &wg, i)
		wrk.WithOption(
			sender.Quiet(args.quiet), sender.Dryrun(args.dryRun),
			sender.ExtractToFile(args.extract2file), sender.MatchReqID(args.reqID),
			sender.ExitOnFirstError(args.exitOnFirstError), sender.MinDelayMS(args.minDelayMs),
			sender.OutputDirectory(args.outputDir),
		)
		wg.Add(1)
		go wrk.Run()
	}

	bytesRead := 0
Loop:
	for {
		var umr *request.UnmarshalledRequest
		var n int
		umr, err = rf.GetNextRequest(false)
		if err != nil {
			if err == io.EOF { // only valid non-error "error" - signifies end of file.
				err = nil
				break Loop
			}
			err = errors.Wrapf(err, "corrupted replay file after %d bytes\n", bytesRead)
			log.Printf("%+v", err) // early print here is intentional (in case we get stuck at wg.Wait() below)
			break Loop
		}
		bytesRead += n

		if args.testIntegrity {
			req := umr.Request()
			fmt.Printf("ID: %s\n", req.Id())
			umr.Release()
		} else {
			select {
			case reqChan <- umr:
				numRequestsMade++
				if args.numRequests > 0 && numRequestsMade >= args.numRequests {
					break Loop
				}
			case <-errorRespChan:
				err = errors.New("Received exit signal from one thread")
				// this error will be returned to the caller
				log.Printf("%+v", err) // early print here is intentional (in case we get stuck at wg.Wait() below)
				break Loop
			}
		}

	}

	log.Printf("Closing channel.")
	close(reqChan)
	log.Printf("Waiting for all threads to finish")
	wg.Wait()
	log.Printf("All threads completed. Total requests = %d", numRequestsMade)

	return err
}
