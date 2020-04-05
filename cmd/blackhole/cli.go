/*
Copyright 2020 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in
accordance with the terms of the Adobe license agreement accompanying
it. If you have received this file from a source other than Adobe,
then your use, modification, or distribution of it requires the prior
written permission of Adobe.
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
}

func processCmdline() (args cmdArgs, err error) {

	// usage is customized to include Version number
	var usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s (Build ts: %s):\n", os.Args[0], buildTS)
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
