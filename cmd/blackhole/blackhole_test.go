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
	"context"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"testing"

	"github.com/pkg/errors"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

// serve serves http request using provided fasthttp handler
func serve(server *fasthttp.Server, req *http.Request, count int) (err error) {
	ln := fasthttputil.NewInmemoryListener()
	defer ln.Close()

	go func() {
		err := server.Serve(ln)
		if err != nil {
			log.Fatalf("%+v", errors.Wrapf(err, "failed to serve"))
		}
	}()

	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return ln.Dial()
			},
		},
	}

	for i := 0; i < count; i++ {
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("%+v", errors.Wrapf(err, "request failed with network error"))
		}
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("%+v", errors.Wrapf(err, "request failed with error"))
		}
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("%+v", errors.Wrapf(err, "unable to read body"))
		}
		resp.Body.Close()
	}
	return nil
}

// Example usage
func TestHandler(t *testing.T) {

	reInitGlobals()
	args := cmdArgs{outputDir: "/tmp", numThreads: 5}

	var rc = &runtimeContext{}
	server := &fasthttp.Server{
		Handler: fastHTTPHandler,
	}
	rc.servers = append(rc.servers, server)
	initRunTimeContext(rc, args)
	setupCleanupHandlers(rc, args)
	setupWorkflowHandlers(rc, args)

	r, err := http.NewRequest("POST", "http://127.0.0.1/", nil)
	if err != nil {
		t.Error(err)
	}

	err = serve(server, r, 1)
	if err != nil {
		t.Error(err)
	}

	err = shutDown(rc)
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkBHSave(b *testing.B) {

	log.SetOutput(ioutil.Discard)
	reInitGlobals()
	args := cmdArgs{outputDir: "/tmp", numThreads: 5}
	var rc = &runtimeContext{}

	server := &fasthttp.Server{
		Handler: fastHTTPHandler,
	}
	rc.servers = append(rc.servers, server)

	initRunTimeContext(rc, args)
	setupCleanupHandlers(rc, args)
	setupWorkflowHandlers(rc, args)

	r, err := http.NewRequest("POST", "http://127.0.0.1/", nil)
	if err != nil {
		b.Error(err)
	}

	err = serve(server, r, b.N)
	if err != nil {
		b.Error(err)
	}

	err = shutDown(rc)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkBHNoSave(b *testing.B) {

	log.SetOutput(ioutil.Discard)
	reInitGlobals()
	args := cmdArgs{}
	var rc = &runtimeContext{}

	server := &fasthttp.Server{
		Handler: fastHTTPHandler,
	}
	rc.servers = append(rc.servers, server)

	initRunTimeContext(rc, args)
	setupCleanupHandlers(rc, args)
	setupWorkflowHandlers(rc, args)

	r, err := http.NewRequest("POST", "http://127.0.0.1/", nil)
	if err != nil {
		b.Error(err)
	}

	err = serve(server, r, b.N)
	if err != nil {
		b.Error(err)
	}

	err = shutDown(rc)
	if err != nil {
		b.Error(err)
	}
}
