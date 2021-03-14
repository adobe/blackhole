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

package main

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adobe/blackhole/lib/request"
	dprofile "github.com/pkg/profile"
	"github.com/valyala/fasthttp"
)

type runtimeContext struct {
	interruptChan chan os.Signal // handle graceful shutdown for stopping profile
	exitChans     []chan bool    // to shutdown (ask them to exit) goroutines on interrupt
	counters      []int64
	wgConsumers   sync.WaitGroup // needs to be global for interrupt-handler to wait on recorder-threads to exit
	outDir        string
	compress      bool
	bufferSize    int
	servers       []*fasthttp.Server
	activeProfile interface{ Stop() }
	// Because of the need to Flush and Close the profiler output
	// from the interrupt handler below, this has to managed as a module/global
}

// A channel used to send incoming messages for Archival.
// Needs to be global to be available from http handler.
// requestConsumer() is the archiver
var recordReqChan chan *request.MarshalledRequest

func initRunTimeContext(rc *runtimeContext, args cmdArgs) {
	for i := 0; i < args.numThreads; i++ {
		rc.exitChans = append(rc.exitChans, make(chan bool, 1)) // Docs recommend a buffer of 1
	}
	rc.interruptChan = make(chan os.Signal, 1) // Docs recommend a buffer of 1
	rc.outDir = args.outputDir
	rc.bufferSize = args.bufferSize
	rc.compress = args.compress
	rc.activeProfile = nil
	rc.counters = make([]int64, args.numThreads)
}

func main() {

	var err error
	var rc = &runtimeContext{}

	args, err := processCmdline()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	initRunTimeContext(rc, args)
	log.Printf("Built: %s", buildTS)

	err = loadConfig()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	if args.cpuProfile {
		rc.activeProfile = dprofile.Start(dprofile.CPUProfile, dprofile.NoShutdownHook)
		defer rc.activeProfile.Stop()
	} else if args.memProfile {
		rc.activeProfile = dprofile.Start(dprofile.MemProfile, dprofile.NoShutdownHook)
		defer rc.activeProfile.Stop()
	} else if args.mutexProfile {
		rc.activeProfile = dprofile.Start(dprofile.MutexProfile, dprofile.NoShutdownHook)
		defer rc.activeProfile.Stop()
	} else if args.blockProfile {
		rc.activeProfile = dprofile.Start(dprofile.BlockProfile, dprofile.NoShutdownHook)
		defer rc.activeProfile.Stop()
	}

	cfg, err := loadTLSConfig()
	if err != nil {
		log.Fatalf("TLS setup failed:%+v", err)
	}

	if args.skip_stats && args.outputDir != "" {
		log.Printf("Skipping stats is useful only if you also avoid saving requests")
		args.skip_stats = false
	}

	setupCleanupHandlers(rc, args)
	if !args.skip_stats {
		setupWorkflowHandlers(rc, args)
	}
	lns, err := createListeners(cfg)
	if err != nil {
		log.Fatalf("Unable to start listeners: %+v", err)
	}
	startServers(rc, lns)

	log.Printf("main(): Waiting for all reader threads to exit")
	rc.wgConsumers.Wait()
}

func reInitGlobals() { // Used for testing
	recordReqChan = nil
}

func statsPrinter(rc *runtimeContext) {

	tickerPrint := time.NewTicker(5 * time.Second) // Flush at least once in 5 seconds
	defer tickerPrint.Stop()
	var priorCount int64 = 0
	priorStatTime := time.Now()
	for range tickerPrint.C {
		var totalCount int64 = 0
		for i := range rc.counters {
			totalCount += atomic.LoadInt64(&rc.counters[i])
		}
		log.Printf("Aggregate: %d requests received in last (%.2f seconds). Total %d so far",
			totalCount-priorCount,
			time.Since(priorStatTime).Seconds(),
			totalCount)
		priorCount = totalCount
		priorStatTime = time.Now()
	}
}

func setupWorkflowHandlers(rc *runtimeContext, args cmdArgs) {

	go statsPrinter(rc)

	rc.wgConsumers.Add(args.numThreads)
	recordReqChan = make(chan *request.MarshalledRequest, 10000)
	for i := 0; i < args.numThreads; i++ {
		go func(j int) {
			err := requestConsumer(j, rc, args.outputDir == "")
			if err != nil {
				log.Fatalf("%+v", err)
			}
		}(i)
	}

}
