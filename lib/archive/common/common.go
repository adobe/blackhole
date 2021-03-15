/*
Copyright 2020 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package common

import (
	"log"
	"path"
	"sync"
)

func UploadProgressPrinter(statChan chan int64, fileName string, fileSize int64, wg *sync.WaitGroup) {

	defer wg.Done()
	total := int64(0)
	lastPrinted := int64(0)
	fileName = path.Base(fileName)
	for bytesTransferred := range statChan {
		if bytesTransferred > total {
			diff := bytesTransferred - lastPrinted
			if diff > 1_000_000 {
				log.Printf("%s: %d/%d (%2.02f %%)",
					fileName, bytesTransferred, fileSize,
					float64(bytesTransferred*100.0)/float64(fileSize))
				lastPrinted = bytesTransferred
			}
		} else {
			log.Printf("WARNING: Previous attempt failed for %s. Bytes transferred went from %d to %d",
				fileName, total, bytesTransferred)
		}
		total = bytesTransferred
	}
}