---
sidebar_position: 9
---

# C FFI Integration

Blaze provides a C Foreign Function Interface (FFI) for embedding in C, C++, and other languages that support C bindings.

## Building with FFI Support

```bash
cargo build --release --features c-ffi
```

This produces:
- `target/release/libblaze.so` (Linux)
- `target/release/libblaze.dylib` (macOS)
- `target/release/blaze.dll` (Windows)
- `include/blaze.h` (header file)

## Header File

The C API is defined in `include/blaze.h`:

```c
#ifndef BLAZE_H
#define BLAZE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque types
typedef struct BlazeConnection BlazeConnection;
typedef struct BlazeResult BlazeResult;
typedef struct BlazePreparedStatement BlazePreparedStatement;

// Connection management
BlazeConnection* blaze_connection_new(void);
BlazeConnection* blaze_connection_with_config(
    uint32_t batch_size,
    uint64_t memory_limit,
    uint32_t num_threads
);
void blaze_connection_free(BlazeConnection* conn);

// Table registration
int blaze_register_csv(
    BlazeConnection* conn,
    const char* name,
    const char* path
);
int blaze_register_parquet(
    BlazeConnection* conn,
    const char* name,
    const char* path
);

// Query execution
BlazeResult* blaze_query(BlazeConnection* conn, const char* sql);
int blaze_execute(BlazeConnection* conn, const char* sql);

// Prepared statements
BlazePreparedStatement* blaze_prepare(
    BlazeConnection* conn,
    const char* sql
);
BlazeResult* blaze_prepared_execute(
    BlazePreparedStatement* stmt,
    const char** params,
    int param_count
);
void blaze_prepared_free(BlazePreparedStatement* stmt);

// Result handling
int64_t blaze_result_row_count(const BlazeResult* result);
int32_t blaze_result_column_count(const BlazeResult* result);
const char* blaze_result_column_name(const BlazeResult* result, int32_t col);
const char* blaze_result_column_type(const BlazeResult* result, int32_t col);

// Value access
bool blaze_result_is_null(const BlazeResult* result, int64_t row, int32_t col);
int64_t blaze_result_get_int(const BlazeResult* result, int64_t row, int32_t col);
double blaze_result_get_double(const BlazeResult* result, int64_t row, int32_t col);
const char* blaze_result_get_string(const BlazeResult* result, int64_t row, int32_t col);

// Memory management
void blaze_result_free(BlazeResult* result);

// Error handling
const char* blaze_last_error(void);
void blaze_clear_error(void);

#ifdef __cplusplus
}
#endif

#endif // BLAZE_H
```

## Basic Usage (C)

```c
#include <stdio.h>
#include "blaze.h"

int main() {
    // Create connection
    BlazeConnection* conn = blaze_connection_new();
    if (!conn) {
        fprintf(stderr, "Failed to create connection: %s\n", blaze_last_error());
        return 1;
    }

    // Register a CSV file
    if (blaze_register_csv(conn, "users", "data/users.csv") != 0) {
        fprintf(stderr, "Failed to register CSV: %s\n", blaze_last_error());
        blaze_connection_free(conn);
        return 1;
    }

    // Execute query
    BlazeResult* result = blaze_query(conn, "SELECT * FROM users LIMIT 10");
    if (!result) {
        fprintf(stderr, "Query failed: %s\n", blaze_last_error());
        blaze_connection_free(conn);
        return 1;
    }

    // Print results
    int64_t rows = blaze_result_row_count(result);
    int32_t cols = blaze_result_column_count(result);

    // Print header
    for (int32_t c = 0; c < cols; c++) {
        printf("%s\t", blaze_result_column_name(result, c));
    }
    printf("\n");

    // Print rows
    for (int64_t r = 0; r < rows; r++) {
        for (int32_t c = 0; c < cols; c++) {
            if (blaze_result_is_null(result, r, c)) {
                printf("NULL\t");
            } else {
                const char* type = blaze_result_column_type(result, c);
                if (strcmp(type, "Int64") == 0) {
                    printf("%lld\t", blaze_result_get_int(result, r, c));
                } else if (strcmp(type, "Float64") == 0) {
                    printf("%.2f\t", blaze_result_get_double(result, r, c));
                } else {
                    printf("%s\t", blaze_result_get_string(result, r, c));
                }
            }
        }
        printf("\n");
    }

    // Cleanup
    blaze_result_free(result);
    blaze_connection_free(conn);

    return 0;
}
```

### Compiling

```bash
# Linux
gcc -o example example.c -L./target/release -lblaze -I./include

# macOS
clang -o example example.c -L./target/release -lblaze -I./include

# Run (Linux)
LD_LIBRARY_PATH=./target/release ./example

# Run (macOS)
DYLD_LIBRARY_PATH=./target/release ./example
```

## C++ Usage

```cpp
#include <iostream>
#include <memory>
#include <stdexcept>
#include "blaze.h"

// RAII wrapper
class BlazeDB {
public:
    BlazeDB() : conn_(blaze_connection_new()) {
        if (!conn_) {
            throw std::runtime_error(blaze_last_error());
        }
    }

    ~BlazeDB() {
        if (conn_) blaze_connection_free(conn_);
    }

    void registerCsv(const std::string& name, const std::string& path) {
        if (blaze_register_csv(conn_, name.c_str(), path.c_str()) != 0) {
            throw std::runtime_error(blaze_last_error());
        }
    }

    class Result {
    public:
        explicit Result(BlazeResult* res) : res_(res) {
            if (!res_) throw std::runtime_error(blaze_last_error());
        }
        ~Result() { if (res_) blaze_result_free(res_); }

        int64_t rowCount() const { return blaze_result_row_count(res_); }
        int32_t columnCount() const { return blaze_result_column_count(res_); }

        std::string columnName(int32_t col) const {
            return blaze_result_column_name(res_, col);
        }

        bool isNull(int64_t row, int32_t col) const {
            return blaze_result_is_null(res_, row, col);
        }

        int64_t getInt(int64_t row, int32_t col) const {
            return blaze_result_get_int(res_, row, col);
        }

        double getDouble(int64_t row, int32_t col) const {
            return blaze_result_get_double(res_, row, col);
        }

        std::string getString(int64_t row, int32_t col) const {
            return blaze_result_get_string(res_, row, col);
        }

    private:
        BlazeResult* res_;
    };

    Result query(const std::string& sql) {
        return Result(blaze_query(conn_, sql.c_str()));
    }

private:
    BlazeConnection* conn_;
};

int main() {
    try {
        BlazeDB db;
        db.registerCsv("users", "data/users.csv");

        auto result = db.query("SELECT name, age FROM users WHERE age > 25");

        std::cout << "Rows: " << result.rowCount() << std::endl;
        for (int64_t r = 0; r < result.rowCount(); r++) {
            std::cout << result.getString(r, 0) << ": "
                      << result.getInt(r, 1) << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
```

## Prepared Statements

```c
#include "blaze.h"

void query_by_id(BlazeConnection* conn, int id) {
    // Prepare statement
    BlazePreparedStatement* stmt = blaze_prepare(
        conn,
        "SELECT * FROM users WHERE id = $1"
    );
    if (!stmt) {
        fprintf(stderr, "Prepare failed: %s\n", blaze_last_error());
        return;
    }

    // Execute with parameter
    char id_str[32];
    snprintf(id_str, sizeof(id_str), "%d", id);
    const char* params[] = { id_str };

    BlazeResult* result = blaze_prepared_execute(stmt, params, 1);
    if (result) {
        printf("Found %lld rows\n", blaze_result_row_count(result));
        blaze_result_free(result);
    }

    blaze_prepared_free(stmt);
}
```

## Python ctypes Example

```python
import ctypes
from ctypes import c_void_p, c_char_p, c_int, c_int32, c_int64, c_double, c_bool

# Load library
lib = ctypes.CDLL('./target/release/libblaze.so')

# Define function signatures
lib.blaze_connection_new.restype = c_void_p
lib.blaze_connection_free.argtypes = [c_void_p]

lib.blaze_register_csv.argtypes = [c_void_p, c_char_p, c_char_p]
lib.blaze_register_csv.restype = c_int

lib.blaze_query.argtypes = [c_void_p, c_char_p]
lib.blaze_query.restype = c_void_p

lib.blaze_result_row_count.argtypes = [c_void_p]
lib.blaze_result_row_count.restype = c_int64

lib.blaze_result_column_count.argtypes = [c_void_p]
lib.blaze_result_column_count.restype = c_int32

lib.blaze_result_get_string.argtypes = [c_void_p, c_int64, c_int32]
lib.blaze_result_get_string.restype = c_char_p

lib.blaze_result_free.argtypes = [c_void_p]

lib.blaze_last_error.restype = c_char_p

# Usage
conn = lib.blaze_connection_new()
if not conn:
    print(f"Error: {lib.blaze_last_error().decode()}")
    exit(1)

lib.blaze_register_csv(conn, b"users", b"data/users.csv")

result = lib.blaze_query(conn, b"SELECT * FROM users LIMIT 5")
if result:
    rows = lib.blaze_result_row_count(result)
    cols = lib.blaze_result_column_count(result)

    for r in range(rows):
        for c in range(cols):
            val = lib.blaze_result_get_string(result, r, c)
            print(val.decode() if val else "NULL", end="\t")
        print()

    lib.blaze_result_free(result)

lib.blaze_connection_free(conn)
```

## Ruby FFI Example

```ruby
require 'ffi'

module Blaze
  extend FFI::Library
  ffi_lib './target/release/libblaze.so'

  attach_function :blaze_connection_new, [], :pointer
  attach_function :blaze_connection_free, [:pointer], :void
  attach_function :blaze_register_csv, [:pointer, :string, :string], :int
  attach_function :blaze_query, [:pointer, :string], :pointer
  attach_function :blaze_result_row_count, [:pointer], :int64
  attach_function :blaze_result_column_count, [:pointer], :int32
  attach_function :blaze_result_get_string, [:pointer, :int64, :int32], :string
  attach_function :blaze_result_free, [:pointer], :void
  attach_function :blaze_last_error, [], :string
end

conn = Blaze.blaze_connection_new
Blaze.blaze_register_csv(conn, "users", "data/users.csv")

result = Blaze.blaze_query(conn, "SELECT * FROM users LIMIT 5")
rows = Blaze.blaze_result_row_count(result)
cols = Blaze.blaze_result_column_count(result)

rows.times do |r|
  cols.times do |c|
    print "#{Blaze.blaze_result_get_string(result, r, c)}\t"
  end
  puts
end

Blaze.blaze_result_free(result)
Blaze.blaze_connection_free(conn)
```

## Error Handling

The FFI uses thread-local error storage:

```c
BlazeResult* result = blaze_query(conn, "SELECT * FROM nonexistent");
if (!result) {
    const char* error = blaze_last_error();
    fprintf(stderr, "Error: %s\n", error);
    blaze_clear_error();  // Clear error state
}
```

## Thread Safety

- Each `BlazeConnection` should be used from a single thread
- Multiple connections can be used from different threads
- Error messages are thread-local

## Memory Management

- Always pair `blaze_*_new()` with `blaze_*_free()`
- String pointers from `blaze_result_get_string()` are valid until `blaze_result_free()`
- Copy strings if needed beyond result lifetime

## Performance Tips

1. **Reuse connections**: Connection creation has overhead
2. **Use prepared statements**: For repeated queries
3. **Batch operations**: Process multiple rows at once
4. **Use Parquet**: More efficient than CSV for large files

## Linking Considerations

### Static Linking

```bash
# Build static library
cargo build --release --features c-ffi

# Link statically
gcc -o example example.c ./target/release/libblaze.a -lpthread -ldl -lm
```

### CMake Integration

```cmake
cmake_minimum_required(VERSION 3.10)
project(blaze_example)

add_executable(example example.c)

target_include_directories(example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_directories(example PRIVATE ${CMAKE_SOURCE_DIR}/target/release)
target_link_libraries(example blaze pthread dl m)
```
