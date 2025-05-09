use anyhow::Result;
use std::process::{Command, Stdio};
use std::io::Write;
use tempfile::NamedTempFile;
use std::fs;

/// Test that the CLI can be launched with basic info command
#[test]
fn test_cli_info_command() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Run the CLI with info command
    let output = Command::new("target/debug/bnql")
        .args(["info"])
        .output()?;
    
    assert!(output.status.success(), "CLI info command failed");
    
    let output_str = String::from_utf8(output.stdout)?;
    assert!(output_str.contains("BayunDB Information:"), "Expected info output not found");
    assert!(output_str.contains("Database file:"), "Database file info not found");
    
    Ok(())
}

/// Test that the CLI can execute a query and format results
#[test]
fn test_cli_query_execution() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Create a temporary database file
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let log_dir = temp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;
    
    // Run the CLI with a simple query
    let output = Command::new("target/debug/bnql")
        .args([
            "--db-path", &db_path.to_string_lossy(),
            "--log-dir", &log_dir.to_string_lossy(),
            "query",
            "SELECT * FROM test_table"
        ])
        .output()?;
    
    assert!(output.status.success(), "CLI query execution failed");
    
    let output_str = String::from_utf8(output.stdout)?;
    // test_table is a stub table that should have rows in the test environment
    assert!(output_str.contains("id"), "Column header not found in query result");
    assert!(output_str.contains("name"), "Column header not found in query result");
    assert!(output_str.contains("rows)"), "Row count not found in query result");
    
    Ok(())
}

/// Test the CLI shell functionality with input redirection
#[test]
fn test_cli_shell_interaction() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Create a temporary database file
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let log_dir = temp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;
    
    // Create a temporary file with SQL commands
    let mut input_file = NamedTempFile::new()?;
    writeln!(input_file, "SELECT * FROM test_table WHERE id < 5;")?;
    writeln!(input_file, "help")?;
    writeln!(input_file, "exit")?;
    input_file.flush()?;
    
    // Run the CLI in shell mode with input redirection
    let output = Command::new("target/debug/bnql")
        .args([
            "--db-path", &db_path.to_string_lossy(),
            "--log-dir", &log_dir.to_string_lossy(),
        ])
        .stdin(Stdio::from(input_file.reopen()?))
        .output()?;
    
    assert!(output.status.success(), "CLI shell interaction failed");
    
    let output_str = String::from_utf8(output.stdout)?;
    assert!(output_str.contains("Welcome to BayunDB CLI"), "Welcome message not found");
    assert!(output_str.contains("id"), "Column header not found in query result");
    assert!(output_str.contains("Available commands:"), "Help message not found");
    assert!(output_str.contains("Goodbye!"), "Exit message not found");
    
    Ok(())
}

/// Test that the CLI handles errors gracefully
#[test]
fn test_cli_error_handling() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Run with invalid query
    let output = Command::new("target/debug/bnql")
        .args(["query", "SELECT * FROM nonexistent_table"])
        .output()?;
    
    // Note: This command will "succeed" in the sense that the process exits normally,
    // but the query itself should generate an error message
    assert!(output.status.success(), "CLI process failed unexpectedly");
    
    let output_str = String::from_utf8(output.stderr)?;
    assert!(output_str.contains("Error") || 
            String::from_utf8(output.stdout)?.contains("Error"), 
            "Error message not found for nonexistent table");
    
    Ok(())
}

/// Test CLI help output
#[test]
fn test_cli_help_output() -> Result<()> {
    // Run CLI with --help
    let output = Command::new("target/debug/bnql")
        .args(["--help"])
        .output()?;
    
    assert!(output.status.success(), "CLI help command failed");
    
    let output_str = String::from_utf8(output.stdout)?;
    assert!(output_str.contains("Usage:"), "Help usage section not found");
    assert!(output_str.contains("Options:"), "Help options section not found");
    assert!(output_str.contains("Commands:"), "Help commands section not found");
    
    Ok(())
}

/// Test that the CLI handles CREATE TABLE statements appropriately
#[test]
fn test_cli_create_table() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Create a temporary database file
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_create.db");
    let log_dir = temp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;
    
    // Create a temporary file with SQL commands
    let mut input_file = NamedTempFile::new()?;
    
    // Write CREATE TABLE command
    writeln!(input_file, "CREATE TABLE test_products (id INTEGER, name TEXT, price FLOAT);")?;
    
    // Exit command
    writeln!(input_file, "exit")?;
    input_file.flush()?;
    
    // Run the CLI in shell mode with input redirection
    let output = Command::new("target/debug/bnql")
        .args([
            "--db-path", &db_path.to_string_lossy(),
            "--log-dir", &log_dir.to_string_lossy(),
        ])
        .stdin(Stdio::from(input_file.reopen()?))
        .output()?;
    
    assert!(output.status.success(), "CLI create table interaction should not crash");
    
    let output_str = String::from_utf8(output.stdout)?;
    
    // Since CREATE TABLE is not implemented yet in the parser,
    // we expect an error message indicating the feature is not implemented
    assert!(output_str.contains("Error") || 
            output_str.contains("not implemented"), 
            "Expected error message for unimplemented CREATE TABLE");
    
    Ok(())
}

/// Test CREATE TABLE error handling in CLI
#[test]
fn test_cli_create_table_errors() -> Result<()> {
    // Build the CLI binary
    let status = Command::new("cargo")
        .args(["build", "--bin", "bnql"])
        .status()?;
    
    assert!(status.success(), "Failed to build bnql binary");
    
    // Create a temporary database file
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_create_error.db");
    let log_dir = temp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;
    
    // Create a temporary file with SQL commands with errors
    let mut input_file = NamedTempFile::new()?;
    
    // Two different CREATE TABLE commands (should both fail since feature isn't implemented)
    writeln!(input_file, "CREATE TABLE users (id INTEGER, name TEXT);")?;
    writeln!(input_file, "CREATE TABLE products (id INTEGER, name TEXT, price FLOAT);")?;
    
    // Exit command
    writeln!(input_file, "exit")?;
    input_file.flush()?;
    
    // Run the CLI in shell mode with input redirection
    let output = Command::new("target/debug/bnql")
        .args([
            "--db-path", &db_path.to_string_lossy(),
            "--log-dir", &log_dir.to_string_lossy(),
        ])
        .stdin(Stdio::from(input_file.reopen()?))
        .output()?;
    
    assert!(output.status.success(), "CLI should not crash with CREATE TABLE commands");
    
    let output_str = String::from_utf8(output.stdout)?;
    
    // Check for error messages
    // We expect to see at least two error messages since we tried CREATE TABLE twice
    let error_count = output_str.matches("Error").count() + 
                     output_str.matches("not implemented").count();
    
    assert!(error_count >= 2, "Expected at least two error messages for CREATE TABLE commands");
    
    Ok(())
} 