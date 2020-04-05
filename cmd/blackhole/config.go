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

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

func loadConfig() (err error) {

	viper.SetConfigName("bhconfig") // name of config file (without extension)
	// viper.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/blackhole/")  // path to look for the config file in
	viper.AddConfigPath("$HOME/.blackhole") // call multiple times to add many search paths
	viper.AddConfigPath(".")                // optionally look for config in the working directory

	viper.SetDefault("serve", ([]interface{}{"http://:80"}))
	// viper.SetDefault("serve", ([]string{"http://:80"}))

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Printf("No config file (bhconfig.yaml) found in usual locations (/etc/blackhole/, $HOME/.blackhole, <PWD>)")
		} else {
			return errors.Wrapf(err, "Config file was found, but encountered an error reading it")
		}
	}
	// log.Printf("%+v", viper.Get("serve"))
	// log.Printf("%+v", viper.Get("tls"))

	return nil
}
