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
