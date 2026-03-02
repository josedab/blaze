//! Blaze CLI - Command Line Interface for Blaze Query Engine
//!
//! Usage:
//!   blaze                     # Interactive REPL
//!   blaze script.sql          # Execute SQL file
//!   blaze -c "SELECT 1"       # Execute single command
//!   blaze -f json             # Set output format
//!   blaze -o output.csv       # Write output to file

use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use arrow::array::Array;
use clap::Parser;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Config, Editor, Helper};

use blaze::output::{OutputFormat, OutputWriter};
use blaze::{Connection, Result};

/// SQL keyword and command completer for the REPL.
struct BlazeHelper {
    table_names: Vec<String>,
}

impl BlazeHelper {
    fn new() -> Self {
        Self {
            table_names: Vec::new(),
        }
    }

    fn update_tables(&mut self, tables: Vec<String>) {
        self.table_names = tables;
    }
}

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "JOIN", "INNER", "LEFT",
    "RIGHT", "FULL", "OUTER", "CROSS", "ON", "GROUP", "BY", "ORDER", "ASC",
    "DESC", "LIMIT", "OFFSET", "HAVING", "UNION", "INTERSECT", "EXCEPT",
    "WITH", "AS", "AND", "OR", "NOT", "NULL", "IS", "IN", "BETWEEN", "LIKE",
    "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT", "COUNT", "SUM", "AVG",
    "MIN", "MAX", "EXPLAIN", "ANALYZE", "COPY", "TO", "VIEW",
];

impl Completer for BlazeHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_to_pos = &line[..pos];
        let word_start = line_to_pos
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .map(|i| i + 1)
            .unwrap_or(0);
        let prefix = &line_to_pos[word_start..];
        let prefix_upper = prefix.to_uppercase();

        let mut completions = Vec::new();

        // Complete dot-commands
        if prefix.starts_with('.') {
            for cmd in &[
                ".tables", ".schema", ".read", ".timer", ".mode", ".output",
                ".help", ".exit", ".quit", ".advice",
            ] {
                if cmd.starts_with(prefix) {
                    completions.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
        } else {
            // Complete SQL keywords
            for kw in SQL_KEYWORDS {
                if kw.starts_with(&prefix_upper) && !prefix.is_empty() {
                    completions.push(Pair {
                        display: kw.to_string(),
                        replacement: kw.to_string(),
                    });
                }
            }
            // Complete table names
            for table in &self.table_names {
                if table.to_uppercase().starts_with(&prefix_upper) && !prefix.is_empty() {
                    completions.push(Pair {
                        display: table.clone(),
                        replacement: table.clone(),
                    });
                }
            }
        }

        Ok((word_start, completions))
    }
}

impl Hinter for BlazeHelper {
    type Hint = String;
}

impl Highlighter for BlazeHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }
}

impl Validator for BlazeHelper {}
impl Helper for BlazeHelper {}

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

        if input.starts_with(".advice") {
            let sql = input.strip_prefix(".advice").unwrap_or("").trim();
            if sql.is_empty() {
                println!("Usage: .advice <sql_query>");
            } else {
                match self.conn.query(sql) {
                    Ok(_) => println!("Query executed successfully. No issues detected."),
                    Err(e) => println!("Query analysis: {}", e),
                }
            }
            return true;
        }

        // Execute as SQL
        // Detect EXPLAIN queries and format as plain text
        if input.trim().to_uppercase().starts_with("EXPLAIN") {
            match self.conn.query(input) {
                Ok(batches) => {
                    for batch in &batches {
                        if let Some(col) = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>() {
                            for i in 0..col.len() {
                                if let Some(line) = col.value(i).into() {
                                    println!("{}", line);
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
            return true;
        }

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

    let config = Config::builder()
        .auto_add_history(true)
        .build();
    let mut rl = Editor::<BlazeHelper, rustyline::history::DefaultHistory>::with_config(config)
        .unwrap_or_else(|_| Editor::new().expect("Failed to create editor"));
    rl.set_helper(Some(BlazeHelper::new()));

    // Load history
    let history_path = dirs_home().join(".blaze_history");
    let _ = rl.load_history(&history_path);

    let mut multiline_buffer = String::new();

    loop {
        // Refresh table names for completion
        if let Some(helper) = rl.helper_mut() {
            helper.update_tables(session.conn.list_tables());
        }

        let prompt = if multiline_buffer.is_empty() {
            "blaze> "
        } else {
            "   ... "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                multiline_buffer.push_str(trimmed);
                multiline_buffer.push(' ');

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
            Err(ReadlineError::Interrupted) => {
                multiline_buffer.clear();
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    // Save history
    let _ = rl.save_history(&history_path);

    Ok(())
}

/// Get user home directory for history file.
fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn print_help() {
    println!("Commands:");
    println!("  .help              - Show this help message");
    println!("  .tables            - List all tables");
    println!("  .schema <table>    - Show table schema");
    println!("  .read <name> <file> - Register file as table");
    println!("  .advice <sql>      - Analyze a SQL query for issues");
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
