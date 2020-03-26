#!/bin/bash
# --- Switch to script's base directory.
cd $(dirname ${0})

golangci-lint run
