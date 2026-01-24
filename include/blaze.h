/*
 * Blaze SQL Query Engine - C API
 * Version: 0.1.0
 *
 * This header provides C bindings for the Blaze query engine.
 * All objects returned from blaze_* functions must be freed using
 * the corresponding *_free function.
 *
 * Thread Safety:
 *   - BlazeConnection is NOT thread-safe. Use separate connections per thread.
 *   - BlazeResult can be read from multiple threads but not modified.
 *
 * Example:
 *   BlazeConnection* conn = blaze_connection_new();
 *   BlazeResult* result = blaze_query(conn, "SELECT 1 + 1");
 *   printf("Rows: %lld\n", blaze_result_row_count(result));
 *   blaze_result_free(result);
 *   blaze_connection_free(conn);
 */

#ifndef BLAZE_H
#define BLAZE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Opaque Types
 * ============================================================================ */

/**
 * Opaque handle to a Blaze database connection.
 *
 * All operations on this handle must use the blaze_* functions.
 * Must be freed with blaze_connection_free().
 */
typedef struct BlazeConnection BlazeConnection;

/**
 * Opaque handle to a query result.
 *
 * Contains the result of a query execution including rows and schema.
 * Must be freed with blaze_result_free().
 */
typedef struct BlazeResult BlazeResult;

/* ============================================================================
 * Status Codes
 * ============================================================================ */

/**
 * Result status codes for FFI operations.
 */
typedef enum BlazeStatus {
    /** Operation succeeded */
    BLAZE_OK = 0,
    /** Invalid argument */
    BLAZE_INVALID_ARGUMENT = 1,
    /** SQL syntax error */
    BLAZE_SYNTAX_ERROR = 2,
    /** Query execution error */
    BLAZE_EXECUTION_ERROR = 3,
    /** Table not found */
    BLAZE_TABLE_NOT_FOUND = 4,
    /** Memory allocation failed */
    BLAZE_OUT_OF_MEMORY = 5,
    /** Internal error */
    BLAZE_INTERNAL_ERROR = 6,
    /** Null pointer passed */
    BLAZE_NULL_POINTER = 7,
} BlazeStatus;

/* ============================================================================
 * Connection Management
 * ============================================================================ */

/**
 * Create a new in-memory database connection.
 *
 * @return Pointer to a new BlazeConnection on success, NULL on failure.
 *
 * The returned pointer must be freed with blaze_connection_free().
 */
BlazeConnection* blaze_connection_new(void);

/**
 * Free a database connection.
 *
 * @param conn Connection to free. May be NULL (no-op).
 *
 * After calling this function, conn is invalid and must not be used.
 */
void blaze_connection_free(BlazeConnection* conn);

/**
 * Get the last error message from a connection.
 *
 * @param conn Valid connection pointer.
 * @return Pointer to a null-terminated error string, or NULL if no error.
 *
 * The returned string must be freed with blaze_string_free().
 */
const char* blaze_last_error(const BlazeConnection* conn);

/**
 * Clear the last error on a connection.
 *
 * @param conn Connection to clear error from.
 */
void blaze_clear_error(BlazeConnection* conn);

/* ============================================================================
 * Query Execution
 * ============================================================================ */

/**
 * Execute a SQL query and return results.
 *
 * @param conn Valid connection pointer.
 * @param sql Null-terminated SQL query string.
 * @return Pointer to a new BlazeResult on success, NULL on failure.
 *
 * On failure, check blaze_last_error() for details.
 * The returned pointer must be freed with blaze_result_free().
 */
BlazeResult* blaze_query(BlazeConnection* conn, const char* sql);

/**
 * Execute a SQL statement (DDL like CREATE TABLE, DROP TABLE).
 *
 * @param conn Valid connection pointer.
 * @param sql Null-terminated SQL statement string.
 * @return Number of rows affected (>= 0) on success, negative on error.
 *
 * On error, check blaze_last_error() for details.
 */
int64_t blaze_execute(BlazeConnection* conn, const char* sql);

/* ============================================================================
 * Result Access
 * ============================================================================ */

/**
 * Get the number of rows in a result.
 *
 * @param result Valid result pointer.
 * @return Number of rows, or 0 if result is NULL.
 */
int64_t blaze_result_row_count(const BlazeResult* result);

/**
 * Get the number of columns in a result.
 *
 * @param result Valid result pointer.
 * @return Number of columns, or 0 if result is NULL.
 */
int64_t blaze_result_column_count(const BlazeResult* result);

/**
 * Get the result data as a JSON string.
 *
 * @param result Valid result pointer.
 * @return Null-terminated JSON string, or NULL on error.
 *
 * The returned string must be freed with blaze_string_free().
 */
const char* blaze_result_to_json(const BlazeResult* result);

/**
 * Get the result schema as a JSON string.
 *
 * @param result Valid result pointer.
 * @return Null-terminated JSON string with schema, or NULL if no schema.
 *
 * The returned string must be freed with blaze_string_free().
 */
const char* blaze_result_schema(const BlazeResult* result);

/**
 * Free a result object.
 *
 * @param result Result to free. May be NULL (no-op).
 *
 * After calling this function, result is invalid and must not be used.
 */
void blaze_result_free(BlazeResult* result);

/**
 * Free a string returned by the FFI.
 *
 * @param s String to free. May be NULL (no-op).
 */
void blaze_string_free(char* s);

/* ============================================================================
 * Table Management
 * ============================================================================ */

/**
 * Register JSON data as a table.
 *
 * @param conn Valid connection pointer.
 * @param table_name Null-terminated table name.
 * @param json_data Null-terminated JSON array string.
 * @return Number of rows registered (>= 0) on success, negative on error.
 *
 * Example:
 *   blaze_register_json(conn, "users", "[{\"id\": 1, \"name\": \"Alice\"}]");
 */
int64_t blaze_register_json(
    BlazeConnection* conn,
    const char* table_name,
    const char* json_data
);

/**
 * Drop a table from the connection.
 *
 * @param conn Valid connection pointer.
 * @param table_name Null-terminated table name.
 * @return 0 on success, non-zero on error.
 */
int blaze_drop_table(BlazeConnection* conn, const char* table_name);

/**
 * List all tables in the connection.
 *
 * @param conn Valid connection pointer.
 * @return JSON array of table names, or NULL on error.
 *
 * The returned string must be freed with blaze_string_free().
 * Example return: "[\"users\", \"orders\"]"
 */
char* blaze_list_tables(const BlazeConnection* conn);

/* ============================================================================
 * Version Info
 * ============================================================================ */

/**
 * Get the Blaze version string.
 *
 * @return Static null-terminated version string (e.g., "0.1.0").
 *
 * This string is static and does not need to be freed.
 */
const char* blaze_version(void);

#ifdef __cplusplus
}
#endif

#endif /* BLAZE_H */
