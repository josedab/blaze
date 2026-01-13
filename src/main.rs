//! Blaze CLI - Command Line Interface for Blaze Query Engine

use std::io::{self, BufRead, Write};

use blaze::{Connection, Result};

fn main() -> Result<()> {
    println!("Blaze Query Engine v{}", env!("CARGO_PKG_VERSION"));
    println!("Type 'help' for available commands, 'exit' to quit.\n");

    let conn = Connection::in_memory()?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("blaze> ");
        if stdout.flush().is_err() {
            break; // Output stream closed
        }

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) | Err(_) => break, // EOF or read error
            Ok(_) => {}
        }

        let input = line.trim();

        if input.is_empty() {
            continue;
        }

        match input.to_lowercase().as_str() {
            "exit" | "quit" | ".exit" | ".quit" => {
                println!("Goodbye!");
                break;
            }
            "help" | ".help" => {
                print_help();
                continue;
            }
            ".tables" => {
                let tables = conn.list_tables();
                if tables.is_empty() {
                    println!("No tables registered.");
                } else {
                    for table in tables {
                        println!("  {}", table);
                    }
                }
                continue;
            }
            _ => {}
        }

        // Handle special commands
        if input.starts_with(".schema") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 2 {
                if let Some(schema) = conn.table_schema(parts[1]) {
                    for field in schema.fields() {
                        println!(
                            "  {} {} {}",
                            field.name(),
                            field.data_type(),
                            if field.is_nullable() { "NULL" } else { "NOT NULL" }
                        );
                    }
                } else {
                    println!("Table '{}' not found.", parts[1]);
                }
            } else {
                println!("Usage: .schema <table_name>");
            }
            continue;
        }

        if input.starts_with(".read") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() == 3 {
                let name = parts[1];
                let path = parts[2];

                let result = if path.ends_with(".csv") || path.ends_with(".tsv") {
                    conn.register_csv(name, path)
                } else if path.ends_with(".parquet") || path.ends_with(".pq") {
                    conn.register_parquet(name, path)
                } else {
                    println!("Unknown file format. Use .csv, .tsv, .parquet, or .pq");
                    continue;
                };

                match result {
                    Ok(()) => println!("Registered table '{}'", name),
                    Err(e) => println!("Error: {}", e),
                }
            } else {
                println!("Usage: .read <table_name> <file_path>");
            }
            continue;
        }

        // Execute SQL
        match conn.query(input) {
            Ok(batches) => {
                if batches.is_empty() {
                    println!("(empty result)");
                } else {
                    print_batches(&batches);
                }
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }

    Ok(())
}

fn print_help() {
    println!("Commands:");
    println!("  .tables              - List all tables");
    println!("  .schema <table>      - Show table schema");
    println!("  .read <name> <file>  - Register file as table");
    println!("  exit, quit           - Exit the CLI");
    println!();
    println!("SQL Examples:");
    println!("  SELECT 1 + 1;");
    println!("  CREATE TABLE users (id INT, name VARCHAR);");
    println!("  SELECT * FROM users;");
}

fn print_batches(batches: &[arrow::record_batch::RecordBatch]) {
    use arrow::util::pretty::print_batches;

    if let Err(_) = print_batches(batches) {
        // Fallback to basic output
        for batch in batches {
            println!("Batch: {} rows, {} columns", batch.num_rows(), batch.num_columns());
            for (i, field) in batch.schema().fields().iter().enumerate() {
                println!("  {}: {:?}", field.name(), batch.column(i));
            }
        }
    }
}
