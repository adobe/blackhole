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
	"log"
	"net/http"

	dprofile "github.com/pkg/profile"
	flag "github.com/spf13/pflag"
)

var buildTS string

// these are globals
var args cmdArgs

func main() {

	log.Printf("Built: %s", buildTS)
	processCmdline()

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	if args.cpuProfile {
		defer dprofile.Start(dprofile.CPUProfile).Stop()
	} else if args.memProfile {
		defer dprofile.Start(dprofile.MemProfile).Stop()
	} else if args.mutexProfile {
		defer dprofile.Start(dprofile.MutexProfile).Stop()
	} else if args.blockProfile {
		defer dprofile.Start(dprofile.BlockProfile).Stop()
	}

	files := flag.Args()
	for _, file := range files {
		err := replayFile(file, &args)
		if err != nil {
			log.Fatalf("Playing file %s failed: %v", file, err)
		}
	}
}
