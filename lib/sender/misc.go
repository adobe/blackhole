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

package sender

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/adobe/blackhole/lib/fbr"
	"github.com/pkg/errors"
)

// saveRequest prints the request, in human readable / plain text to a file OR stdout
// mainly used for debugging OR extracting a single request. Header and Body are written
// to separate files to support binary/non-text content in the body
func (wrk *Worker) saveRequest(req *fbr.Request, print bool) (err error) {

	tempDir := wrk.outputDir

	var fp *os.File
	var headerName, bodyFileName string
	if !print {
		fp, err = ioutil.TempFile(tempDir, "request_header_")
		if err != nil {
			return errors.Wrapf(err, "Unable to create output file for header")
		}
		headerName = fp.Name()
		bodyFileName = strings.Replace(headerName, "_header_", "_body_", 1)
		defer fp.Close()
	} else {
		fp = os.Stdout
		headerName = "stdout"
		bodyFileName = headerName
	}

	_, err = fp.WriteString(fmt.Sprintf("%s %s\n", req.Method(), req.Uri()))
	if err != nil {
		return err
	}
	_, err = fp.Write(req.Headers())
	if err != nil {
		return err
	}

	if !print {
		fp.Close()
		fp = nil

		fp, err = os.Create(bodyFileName)
		if err != nil {
			return err
		}
		defer fp.Close()
	}

	_, err = fp.Write(req.BodyBytes())
	if err != nil {
		return err
	}

	if !print {
		fp.Close()
		fp = nil
	}
	log.Printf("Request saved in files %s and %s", headerName, bodyFileName)

	return err
}
