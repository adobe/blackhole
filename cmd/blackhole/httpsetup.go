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
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
)

// loadTLSConfig loads TLS option optionally based on `viper` config.
// config is not passed in. `viper` knows where to search for config.
// `viper` config was already loaded from main via `loadConfig()` call.
// returns *tls.Config, nil if TLS is requested
// returns nil, nil if TLS is not requested
func loadTLSConfig() (cfg *tls.Config, err error) {
	tlsConfig := viper.Get("tls")
	if v, ok := tlsConfig.(map[string]interface{}); ok && v != nil {
		certFile, ok := v["cert"].(string)
		if !ok {
			return nil, errors.Errorf("\"tls\" key must include a string subkey \"cert\"")
		}
		privKeyFile, ok := v["privkey"].(string)
		if !ok {
			return nil, errors.Errorf("\"tls\" key must include a string subkey \"privkey\"")
		}
		if certFile == "" {
			return nil, errors.Wrapf(err, "\"tls\" key must have a subkey \"cert\" with certifcate PEM filepath")
		}
		if privKeyFile == "" {
			return nil, errors.Wrapf(err, "\"tls\" key must have a subkey \"privkey\" with private key PEM filepath")
		}
		cert, err := tls.LoadX509KeyPair(privKeyFile, privKeyFile)
		if err != nil {
			return nil, errors.Wrapf(err, "Error loading cert=%s key=%s",
				certFile, privKeyFile)
		}
		cfg := &tls.Config{}
		cfg.Certificates = append(cfg.Certificates, cert)
		return cfg, nil
	}

	log.Printf("INFO: No TLS certificate configured")

	return nil, nil
}

// createListeners creates listeners for each of the addresses
// configured in the `serve` setting.
func createListeners(cfg *tls.Config) (lns []net.Listener, err error) {

	serveConfig := viper.Get("serve")
	if addresses, ok := serveConfig.([]interface{}); ok {
		for si, sv := range addresses {
			if serveURL, ok := sv.(string); ok {
				addr, err := url.Parse(serveURL)
				if err != nil {
					return nil, errors.Wrapf(err,
						"Error in parsing url #%d under `serve`: %s", si, serveURL)
				}
				port := 80 // default http
				if addr.Scheme == "https" {
					if cfg == nil {
						return nil, errors.Errorf(
							"No TLS configuration available for url #%d under `serve`: %s", si, serveURL)
					}
					port = 443 // default https
				}
				portS := addr.Port()
				if portS != "" {
					v, err := strconv.Atoi(portS)
					if err != nil {
						return nil, errors.Wrapf(err,
							"Error in parse port from url #%d under `serve`: %s", si, serveURL)
					}
					port = v
				}
				lnAddr := fmt.Sprintf(":%d", port)
				lnHTTP, err := net.Listen("tcp4", lnAddr)
				if err != nil {
					return nil, errors.Wrapf(err,
						"Error in net.Listen for >%s< from url #%d under `serve`: %s",
						lnAddr, si, serveURL)
				}
				if addr.Scheme == "https" {
					if cfg != nil {
						lnHTTPS := tls.NewListener(lnHTTP, cfg)
						lns = append(lns, lnHTTPS)
					} else {
						return nil, errors.Errorf(
							"No TLS configuration available for url #%d under `serve`: %s", si, serveURL)
					}
				} else {
					lns = append(lns, lnHTTP)
				}
			} else {
				return nil, errors.Errorf(
					"\"serve\" key must contain a *list* of urls of the form `https://www.foobar.com` or `http://127.0.0.1:4587/`")
			}
		}
	} else {
		return nil, errors.Errorf("\"serve\" key must contain a *list* of urls of the form `https://www.foobar.com` or `http://127.0.0.1:4587/`")
	}
	return lns, nil
}

// start a server (goroutine) for each of the listeners
func startServers(rc *runtimeContext, lns []net.Listener) {

	var wg sync.WaitGroup
	for _, ln := range lns {
		srv := &fasthttp.Server{
			Handler: fastHTTPHandler,
		}
		rc.servers = append(rc.servers, srv)
		wg.Add(1)
		go func(_ln net.Listener, _wg *sync.WaitGroup) {
			defer _wg.Done()
			err := srv.Serve(_ln)
			if err != nil {
				log.Fatalf("http server failed with error: %+v", err)
			}
		}(ln, &wg)
	}
	wg.Wait()
}
