@echo off
REM Build the CLI
cargo build --bin bnql

REM Run the CLI with any passed arguments
target\debug\bnql.exe %* 