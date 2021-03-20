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
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

func loadConfig(rc *runtimeContext) (err error) {

	viper.SetConfigName("bhconfig") // name of config file (without extension)
	// viper.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/blackhole/")  // path to look for the config file in
	viper.AddConfigPath("$HOME/.blackhole") // call multiple times to add many search paths
	viper.AddConfigPath(".")                // optionally look for config in the working directory

	viper.SetDefault("serve", ([]interface{}{"http://:80"}))
	// viper.SetDefault("serve", ([]string{"http://:80"}))

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			rc.logger.Warn("No config file (bhconfig.yaml) found in usual locations (/etc/blackhole/, $HOME/.blackhole, <PWD>)")
		} else {
			return errors.Wrapf(err, "Config file was found, but encountered an error reading it")
		}
	}
	return nil
}
