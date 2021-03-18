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

// Package request provides functionality to create an manage individual
// HTTP requests. Requests are serialized using Flatbuffers (See request.fbs for schema)
// This package is to be used in conjunction with the file package
package request

import (
	"strconv"
	"sync"
	"time"

	"github.com/adobe/blackhole/lib/fbr"
	"github.com/adobe/blackhole/lib/slicehacks"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/valyala/fasthttp"
)

var arPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &MarshalledRequest{fb: flatbuffers.NewBuilder(2048)}
	},
}

var idPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		v := make([]byte, 50) // FH-<64-bit-decimal>-<64-bit-decimal>
		// this is just initial size - doesn't need to be accurate.
		return v
	},
}

var requestReadPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		v := make([]byte, 2048)
		return &UnmarshalledRequest{data: v}
	},
}

type MarshalledRequest struct {
	fb *flatbuffers.Builder
}

// UnmarshalledRequest holds an fbr.Request that has just been
// read from storage/archive.
// For flatbuffers, there isn't really an Unmarshal step.
// However I am using the terminology to indicate an object
// that has just been read from storage/archive. It needs a
// buffer that holds the actual DS. Compare this to
// MarshalledRequest where we need a flatbuffers builder instead
// MarshalledRequest is for writing, UnmarshalledRequest is for reading
type UnmarshalledRequest struct {
	data []byte
}

// CreateRequestFromFastHTTPCtx returns *MarshalledRequest ready to be saved
// created from a *fasthttp.RequestCtx . This coupling with fasthttp
// format is to avoid unnecessary copying to other generic formats.
// A *MarshalledRequest contains pointers from a buffer pool.
// You must call `.Release()` on it as soon as you are done with it.
func CreateRequestFromFastHTTPCtx(ctx *fasthttp.RequestCtx) (mr *MarshalledRequest) {
	destURL := ctx.Request.Header.Peek("X-Original-URI")
	if len(destURL) == 0 { // nil or ""
		destURL = ctx.RequestURI()
	}

	id := ctx.Request.Header.Peek("X-Request-ID")
	if len(id) == 0 { // nil or ""
		id = idPool.Get().([]byte)
		defer idPool.Put(id)
		id = append(id[:0], []byte("FH-")...)
		id = strconv.AppendInt(id, time.Now().UnixNano(), 10)
		id = append(id, []byte("-")...)
		id = strconv.AppendUint(id, ctx.ID(), 10)
	}
	return CreateRequest(
		id, ctx.Method(), destURL,
		ctx.Request.Header.RawHeaders(),
		ctx.Request.Body())
}

// CreateRequest returns *MarshalledRequest ready to be saved
// A *MarshalledRequest contains pointers from a buffer pool.
// You must call `.Release()` on it as soon as you are done with it.
func CreateRequest(
	id, method, uri, headers, body []byte) (mr *MarshalledRequest) {
	mr = arPool.Get().(*MarshalledRequest)

	mr.fb.Reset()
	idFB := mr.fb.CreateByteString(id)
	methodFB := mr.fb.CreateByteString(method)
	uriFB := mr.fb.CreateByteString(uri)
	headersFB := mr.fb.CreateByteString(headers)
	bodyFB := mr.fb.CreateByteVector(body)
	fbr.RequestStart(mr.fb)
	fbr.RequestAddId(mr.fb, idFB)
	fbr.RequestAddMethod(mr.fb, methodFB)
	fbr.RequestAddUri(mr.fb, uriFB)
	fbr.RequestAddHeaders(mr.fb, headersFB)
	fbr.RequestAddBody(mr.fb, bodyFB)
	req := fbr.RequestEnd(mr.fb)
	mr.fb.Finish(req)

	return mr
}

// Bytes returns underlying buffer. This is exposed *only* to be passed to an io.Writer
// TODO: Find a better way to encapsulate this
func (mr *MarshalledRequest) Bytes() []byte {
	return mr.fb.FinishedBytes()
}

// Release releases the object back to the pool
func (mr *MarshalledRequest) Release() {
	mr.fb.Reset()
	arPool.Put(mr)
}

// Bytes returns underlying buffer. This is exposed *only* to be passed to readFull / io.ReadFull
// TODO: Find a better way to encapsulate this
func (umr *UnmarshalledRequest) Bytes() []byte {
	return umr.data
}

// Request returns a pointer to the fbr.Request represented by the UnmarshalledRequest
func (umr *UnmarshalledRequest) Request() *fbr.Request {
	return fbr.GetRootAsRequest(umr.data, 0)
}

// Grow can be used to grow the underlying buffer. This is required to for later use with readFull / io.ReadFull
func (umr *UnmarshalledRequest) Grow(size int) {
	umr.data = slicehacks.Grow(umr.data, size)
}

// CreateUMRequest creates an UnmarshalledRequest object (No data in it yet)
// Used by GetNextRequest.
func CreateUMRequest() (umr *UnmarshalledRequest) {
	return requestReadPool.Get().(*UnmarshalledRequest)
}

// Release releases the object back to the pool
func (umr *UnmarshalledRequest) Release() {
	requestReadPool.Put(umr)
}
