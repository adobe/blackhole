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
`blackhole` serves as an HTTP endpoint with optional recording ability

 Usage of ./blackhole (Build ts: 2020-03-23T21:22:49Z):
      --block-profile             (for debug only) Block profile this run
  -b, --buffer-size int           Buffer size (0 - default, unbuffered)
  -c, --compress                  Compress output (or not)
      --cpu-profile               (for debug only) CPU profile this run
      --mem-profile               (for debug only) MEM profile this run
      --mutex-profile             (for debug only) Mutex profile this run
  -o, --output-directory string   Output directory for saved requests
  -t, --recorder-threads int      Number of recorder threads (default 5)
  -v, --verbose                   Verbose output

*/
package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

var buildTS string

type cmdArgs struct {
	cpuProfile   bool
	memProfile   bool
	mutexProfile bool
	blockProfile bool
	verbose      bool
	compress     bool
	bufferSize   int // for performance testing only
	outputDir    string
	numThreads   int
	skip_stats   bool
}

func processCmdline() (args cmdArgs, err error) {

	// usage is customized to include Version number
	var usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s (Build ts: %s)\nPlease see bhconfig_sample.yaml to configure http port and tls\n\n", os.Args[0], buildTS)
		pflag.PrintDefaults()
	}

	pflag.BoolVarP(&args.cpuProfile, "cpu-profile", "", false,
		"(for debug only) CPU profile this run")
	pflag.BoolVarP(&args.memProfile, "mem-profile", "", false,
		"(for debug only) MEM profile this run")
	pflag.BoolVarP(&args.mutexProfile, "mutex-profile", "", false,
		"(for debug only) Mutex profile this run")
	pflag.BoolVarP(&args.blockProfile, "block-profile", "", false,
		"(for debug only) Block profile this run")
	pflag.BoolVarP(&args.verbose, "verbose", "v", false,
		"Verbose output")
	pflag.BoolVarP(&args.skip_stats, "skip-stats", "", false,
		"Skip stats (slight performance increase)")
	pflag.BoolVarP(&args.compress, "compress", "c", false,
		"Compress output (or not)")
	pflag.IntVarP(&args.bufferSize, "buffer-size", "b", 0,
		"Buffer size (0 - default, unbuffered)")
	pflag.IntVarP(&args.numThreads, "recorder-threads", "t", 5, "Number of recorder threads")
	pflag.StringVarP(&args.outputDir, "output-directory", "o", "", "Output directory for saved requests")
	pflag.Usage = usage
	pflag.Parse()

	return args, nil
}
