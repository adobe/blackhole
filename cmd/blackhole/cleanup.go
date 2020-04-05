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
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
)

func setupCleanupHandlers(rc *runtimeContext, args cmdArgs) {
	// Exit sychronization
	// Two tasks
	// - Close profiling if started
	// - Signal exit to all reader threads, if any. Reader threads
	// are only created if data is to be persisted.
	signal.Notify(rc.interruptChan, syscall.SIGINT, syscall.SIGTERM)

	go waitForINTSignal(rc, args)
}

// SINGINT and SIGTERM handler for the app. We need to handle this or we cannot profile the program
// We also need to gracefully close all compressed files written or they will be corrupted.
func waitForINTSignal(rc *runtimeContext, args cmdArgs) {

	s := <-rc.interruptChan
	log.Printf("Received signal: %v", s)
	err := shutDown(rc)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	os.Exit(1)
}

func shutDown(rc *runtimeContext) (err error) {

	if rc.activeProfile != nil {
		rc.activeProfile.Stop()
	}

	// prevent fasthttp from accepting anymore
	// otherwise we cannot reliably close the channel
	for _, srv := range rc.servers {
		err = srv.Shutdown()
		if err != nil {
			return errors.Wrapf(err, "Unable to shutdown HTTP service")
		}
	}

	if recordReqChan != nil {
		close(recordReqChan)
	}

	// **********************************************************
	// WARNING: DO NOT SET recordReqChan CHANNEL TO NIL
	// A nil channel is not equivalent to a "closed" channel.
	// Behavior is completely opposite (block vs release)
	// https://dave.cheney.net/2014/03/19/channel-axioms
	// We want all readers to come out of the for-range/select
	// Initially I had set it to nil to help the
	// logic at < see MARK-b5688e1019ad >
	// but that was a bad idea.
	// **********************************************************

	log.Printf("shutdown: Waiting for all reader threads to exit")
	rc.wgConsumers.Wait()
	log.Printf("All reader threads finished")
	return nil
}
