//! Blaze CLI - Command Line Interface for Blaze Query Engine
//!
//! Usage:
//!   blaze                     # Interactive REPL
//!   blaze script.sql          # Execute SQL file
//!   blaze -c "SELECT 1"       # Execute single command
//!   blaze -f json             # Set output format
//!   blaze -o output.csv       # Write output to file

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use blaze::output::{OutputFormat, OutputWriter};
use blaze::{Connection, Result};

/// Blaze Query Engine - High-performance embedded OLAP database
#[derive(Parser, Debug)]
#[command(name = "blaze")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// SQL file to execute
    #[arg(value_name = "FILE")]
    file: Option<PathBuf>,

    /// Execute a single SQL command
    #[arg(short = 'c', long = "command", value_name = "SQL")]
    command: Option<String>,

    /// Output format: table, csv, json, arrow
    #[arg(short = 'f', long = "format", default_value = "table")]
    format: String,

    /// Output file (default: stdout)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,

    /// Enable query timing
    #[arg(short = 't', long = "timer")]
    timer: bool,

    /// Suppress banner and prompts (for scripting)
    #[arg(short = 'q', long = "quiet")]
    quiet: bool,
}

/// Session state for the REPL
struct Session {
    conn: Connection,
    format: OutputFormat,
    output_path: Option<PathBuf>,
    timer: bool,
    quiet: bool,
}

impl Session {
    fn new(
        conn: Connection,
        format: OutputFormat,
        output_path: Option<PathBuf>,
        timer: bool,
        quiet: bool,
    ) -> Self {
        Self {
            conn,
            format,
            output_path,
            timer,
            quiet,
        }
    }

    fn execute_query(&mut self, sql: &str) -> Result<()> {
        let start = Instant::now();

        match self.conn.query(sql) {
            Ok(batches) => {
                let elapsed = start.elapsed();

                if batches.is_empty() {
                    if !self.quiet {
                        println!("(empty result)");
                    }
                } else {
                    let mut writer = if let Some(ref path) = self.output_path {
                        OutputWriter::file(self.format, path)?
                    } else {
                        OutputWriter::stdout(self.format)
                    };
                    writer.write_batches(&batches)?;

                    if self.timer {
                        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                        println!("\n{} row(s) in {:.3}s", row_count, elapsed.as_secs_f64());
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }

        Ok(())
    }

    fn execute_command(&mut self, input: &str) -> bool {
        // Handle session commands (start with .)
        let input_lower = input.to_lowercase();

        if input_lower == "exit"
            || input_lower == "quit"
            || input_lower == ".exit"
            || input_lower == ".quit"
        {
            if !self.quiet {
                println!("Goodbye!");
            }
            return false;
        }

        if input_lower == "help" || input_lower == ".help" {
            print_help();
            return true;
        }

        if input_lower == ".tables" {
            let tables = self.conn.list_tables();
            if tables.is_empty() {
                println!("No tables registered.");
            } else {
                for table in tables {
                    println!("  {}", table);
                }
            }
            return true;
        }

        if input.starts_with(".timer") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                match parts[1].to_lowercase().as_str() {
                    "on" | "true" | "1" => {
                        self.timer = true;
                        println!("Timer enabled");
                    }
                    "off" | "false" | "0" => {
                        self.timer = false;
                        println!("Timer disabled");
                    }
                    _ => println!("Usage: .timer on|off"),
                }
            } else {
                println!("Timer is {}", if self.timer { "on" } else { "off" });
            }
            return true;
        }

        if input.starts_with(".mode") || input.starts_with(".format") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                match OutputFormat::from_str(parts[1]) {
                    Ok(fmt) => {
                        self.format = fmt;
                        println!("Output format set to: {}", fmt);
                    }
                    Err(e) => println!("Error: {}", e),
                }
            } else {
                println!(
                    "Current format: {}. Usage: .mode csv|json|table|arrow",
                    self.format
                );
            }
            return true;
        }

        if input.starts_with(".output") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                if parts[1] == "stdout" {
                    self.output_path = None;
                    println!("Output reset to stdout");
                } else {
                    self.output_path = Some(PathBuf::from(parts[1]));
                    println!("Output redirected to: {}", parts[1]);
                }
            } else {
                match &self.output_path {
                    Some(path) => println!("Output: {}", path.display()),
                    None => println!("Output: stdout"),
                }
            }
            return true;
        }

        if input.starts_with(".schema") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                if let Some(schema) = self.conn.table_schema(parts[1]) {
                    for field in schema.fields() {
                        println!(
                            "  {} {} {}",
                            field.name(),
                            field.data_type(),
                            if field.is_nullable() {
                                "NULL"
                            } else {
                                "NOT NULL"
                            }
                        );
                    }
                } else {
                    let tables = self.conn.list_tables();
                    if tables.is_empty() {
                        println!("Table '{}' not found. No tables are registered.", parts[1]);
                        println!("Hint: Use .read <name> <file> to load a CSV or Parquet file.");
                    } else {
                        println!("Table '{}' not found. Available tables:", parts[1]);
                        for t in &tables {
                            println!("  {}", t);
                        }
                    }
                }
            } else {
                println!("Usage: .schema <table_name>");
            }
            return true;
        }

        if input.starts_with(".read") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 3 {
                let name = parts[1];
                let path = parts[2];

                let result = if path.ends_with(".csv") || path.ends_with(".tsv") {
                    self.conn.register_csv(name, path)
                } else if path.ends_with(".parquet") || path.ends_with(".pq") {
                    self.conn.register_parquet(name, path)
                } else {
                    println!("Unknown file format. Use .csv, .tsv, .parquet, or .pq");
                    return true;
                };

                match result {
                    Ok(()) => println!("Registered table '{}'", name),
                    Err(e) => println!("Error: {}", e),
                }
            } else {
                println!("Usage: .read <table_name> <file_path>");
            }
            return true;
        }

        // Execute as SQL
        let _ = self.execute_query(input);
        true
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let conn = Connection::in_memory()?;
    let format = OutputFormat::from_str(&cli.format)?;

    let mut session = Session::new(conn, format, cli.output.clone(), cli.timer, cli.quiet);

    // Execute single command if provided
    if let Some(ref cmd) = cli.command {
        session.execute_query(cmd)?;
        return Ok(());
    }

    // Execute SQL file if provided
    if let Some(ref file) = cli.file {
        let content = fs::read_to_string(file).map_err(|e| {
            blaze::BlazeError::execution(format!("Failed to read file '{}': {}", file.display(), e))
        })?;

        // Split by semicolons and execute each statement
        for statement in content.split(';') {
            let sql = statement.trim();
            if !sql.is_empty() && !sql.starts_with("--") {
                session.execute_query(sql)?;
            }
        }
        return Ok(());
    }

    // Interactive REPL mode
    if !cli.quiet {
        println!("Blaze Query Engine v{}", env!("CARGO_PKG_VERSION"));
        println!("Type '.help' for available commands, '.exit' to quit.\n");
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut multiline_buffer = String::new();

    loop {
        // Show prompt
        if !cli.quiet {
            if multiline_buffer.is_empty() {
                print!("blaze> ");
            } else {
                print!("   ... ");
            }
            if stdout.flush().is_err() {
                break;
            }
        }

        // Read input
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) | Err(_) => break, // EOF or read error
            Ok(_) => {}
        }

        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        // Handle multiline input (statements ending with semicolon)
        multiline_buffer.push_str(trimmed);
        multiline_buffer.push(' ');

        // Check if statement is complete (ends with semicolon or is a command)
        let is_command = trimmed.starts_with('.')
            || trimmed.to_lowercase() == "exit"
            || trimmed.to_lowercase() == "quit"
            || trimmed.to_lowercase() == "help";

        if !is_command && !multiline_buffer.trim().ends_with(';') {
            continue;
        }

        let input = multiline_buffer.trim().trim_end_matches(';').to_string();
        multiline_buffer.clear();

        if input.is_empty() {
            continue;
        }

        if !session.execute_command(&input) {
            break;
        }
    }

    Ok(())
}

fn print_help() {
    println!("Commands:");
    println!("  .help              - Show this help message");
    println!("  .tables            - List all tables");
    println!("  .schema <table>    - Show table schema");
    println!("  .read <name> <file> - Register file as table");
    println!("  .timer on|off      - Toggle query timing");
    println!("  .mode <format>     - Set output format (table, csv, json, arrow)");
    println!("  .output <file>     - Redirect output to file (use 'stdout' to reset)");
    println!("  .exit, .quit       - Exit the CLI");
    println!();
    println!("SQL Examples:");
    println!("  SELECT 1 + 1;");
    println!("  CREATE TABLE users (id INT, name VARCHAR);");
    println!("  SELECT * FROM users;");
    println!();
    println!("Command Line Options:");
    println!("  blaze <file.sql>           - Execute SQL file");
    println!("  blaze -c \"SELECT 1\"        - Execute single command");
    println!("  blaze -f json              - Set output format");
    println!("  blaze -o results.csv       - Write output to file");
    println!("  blaze -t                   - Enable query timing");
}
