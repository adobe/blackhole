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
`replay` replays an archive of requests to a different target host. The archive must have been recorded via `blackhole`

 Usage of ./replay:
      --block-profile             (for debug only) Block profile this run
      --cpu-profile               (for debug only) CPU profile this run
  -n, --dryrun                    Unpack and show what is in this file, don't run it
  -x, --exit-on-error             Exit on first error
  -f, --extract-to-file           Extract requests to one file per request. Please use this only with -r limit or -i options
      --mem-profile               (for debug only) MEM profile this run
  -m, --min-delay int             Minimum time in milliseconds to wait before the next request is sent. 0 means no wait. Actual wait till will be max(min-delay, actual-delay)
      --mutex-profile             (for debug only) Mutex profile this run
  -o, --output-directory string   Output directory if -f is used (default ".")
  -q, --quiet                     Run quietly and print only errors
  -i, --reqid string              Run only this particular request identified by an exchange specific format (do dryrun first to see the ids)
  -r, --reqs int                  Send only N requests to the bidder (instead of everything from the file)
  -H, --target-host-port string   Send requests to this host. Example locahost, localhost:8080, host.domain.com
      --test                      Test integrity of the file. Print ID of each request.
  -t, --threads int               Number of request threads (parallel) (default 5)

*/
package main

import (
	"log"

	flag "github.com/spf13/pflag"
)

type cmdArgs struct {
	cpuProfile       bool
	memProfile       bool
	mutexProfile     bool
	blockProfile     bool
	outputDir        string
	targetHost       string
	numRequests      int
	numReqThreads    int
	minDelayMs       int
	dryRun           bool
	exitOnFirstError bool
	reqID            string
	quiet            bool
	extract2file     bool
	testIntegrity    bool
}

func processCmdline() {

	flag.BoolVarP(&args.cpuProfile, "cpu-profile", "", false,
		"(for debug only) CPU profile this run")
	flag.BoolVarP(&args.memProfile, "mem-profile", "", false,
		"(for debug only) MEM profile this run")
	flag.BoolVarP(&args.mutexProfile, "mutex-profile", "", false,
		"(for debug only) Mutex profile this run")
	flag.BoolVarP(&args.blockProfile, "block-profile", "", false,
		"(for debug only) Block profile this run")

	flag.StringVarP(&args.targetHost, "target-host-port", "H", "",
		"Send requests to this host. Example locahost, localhost:8080, host.domain.com")
	flag.IntVarP(&args.numRequests, "reqs", "r", 0,
		"Send only N requests to the bidder (instead of everything from the file)")
	flag.IntVarP(&args.numReqThreads, "threads", "t", 5,
		"Number of request threads (parallel)")
	flag.StringVarP(&args.outputDir, "output-directory", "o", ".",
		"Output directory if -f is used")
	flag.IntVarP(&args.minDelayMs, "min-delay", "m", 0,
		"Minimum time in milliseconds to wait before the next request is sent. 0 means no wait. Actual wait till will be max(min-delay, actual-delay)")
	flag.BoolVarP(&args.dryRun, "dryrun", "n", false,
		"Unpack and show what is in this file, don't run it")
	flag.BoolVarP(&args.exitOnFirstError, "exit-on-error", "x", false,
		"Exit on first error")
	flag.StringVarP(&args.reqID, "reqid", "i", "",
		"Run only this particular request identified by an exchange specific format (do dryrun first to see the ids)")
	flag.BoolVarP(&args.quiet, "quiet", "q", false,
		"Run quietly and print only errors")
	flag.BoolVarP(&args.extract2file, "extract-to-file", "f", false,
		"Extract requests to one file per request. Please use this only with -r limit or -i options")
	flag.BoolVarP(&args.testIntegrity, "test", "", false,
		"Test integrity of the file. Print ID of each request.")

	flag.Parse()

	if args.extract2file {
		args.dryRun = true
	}

	if flag.NArg() == 0 {
		log.Fatalf("Please supply one or more replay-files")
	}
	if !args.dryRun && args.targetHost == "" {
		log.Fatalf("Please supply a bidder targetHostPort (unless you are doing a dryrun)")
	}

	if args.extract2file && !(args.numRequests != 0 || args.reqID != "") {
		log.Fatalf("Please supply a -r or -i option when enabling this flag. Otherwise we will flood filesystem")
	}

}
