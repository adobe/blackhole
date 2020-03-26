/*
Copyright 2020 Adobe
All Rights Reserved.

NOTICE: Adobe permits you to use, modify, and distribute this file in
accordance with the terms of the Adobe license agreement accompanying
it. If you have received this file from a source other than Adobe,
then your use, modification, or distribution of it requires the prior
written permission of Adobe.
*/

package slicehacks

// Grow grows a slice to the required size while retaining
// any existing data.
func Grow(buf []byte, size int) []byte {
	if cap(buf) >= size {
		return buf[:size]
	}

	return append(buf, make([]byte, size-len(buf))...)
}
