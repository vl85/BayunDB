#!/bin/bash

# Build the CLI
cargo build --bin bnql

# Run the CLI with any passed arguments
target/debug/bnql "$@" 