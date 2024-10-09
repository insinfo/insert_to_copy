#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>

#include <pg_query.h>
#include "cJSON.h"

#ifdef _WIN32
#define strncasecmp _strnicmp
#endif

#define INITIAL_BUFFER_CAPACITY 8192
#define MAX_BUFFERED_ROWS 10000  // Adjust this value as needed

// Structures to accumulate data per table
typedef struct DataRow {
    char* data;              // The data row as a string
    struct DataRow* next;    // Pointer to the next data row
} DataRow;

typedef struct TableData {
    char* table_name;        // The full table name (schema.table)
    DataRow* head;           // Head of the linked list of data rows
    DataRow* tail;           // Tail of the linked list for efficient appending
    int row_count;           // Number of accumulated rows
    struct TableData* next;  // Pointer to the next table data in the hash map
} TableData;

#define TABLE_HASH_SIZE 1024
TableData* table_hash[TABLE_HASH_SIZE] = { NULL };

// Function prototypes
char* unescape_single_quotes(const char* input);
char* escape_special_chars(const char* input);
char* skip_comments_and_whitespace(char* input);
void process_parsed_statement(const char* parse_tree_json);
void append_data_row_to_table(TableData* table_data, char* data_row);
void flush_table_data(FILE* output, TableData* table_data);
void flush_accumulated_data(FILE* output);
unsigned int hash_table_name(const char* table_name);
TableData* get_or_create_table_data(const char* table_name);
char* build_data_row(cJSON* row_values);
const char* extract_value_as_string(cJSON* value_item);
bool detect_end_of_statement(const char* statement);
void free_table_data(TableData* table_data);

// Function to unescape single quotes
char* unescape_single_quotes(const char* input) {
    size_t len = strlen(input);
    char* output = malloc(len + 1);
    if (!output) {
        fprintf(stderr, "Memory allocation error\n");
        exit(1);
    }
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (input[i] == '\'' && input[i + 1] == '\'') {
            output[j++] = '\'';
            i++; // Skip the second quote
        }
        else {
            output[j++] = input[i];
        }
    }
    output[j] = '\0';
    return output;
}

// Function to escape tabs and newlines
char* escape_special_chars(const char* input) {
    size_t len = strlen(input);
    // Worst case: every character needs escaping
    char* output = malloc(len * 2 + 1);
    if (!output) {
        fprintf(stderr, "Memory allocation error\n");
        exit(1);
    }
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (input[i] == '\t') {
            output[j++] = '\\';
            output[j++] = 't';
        }
        else if (input[i] == '\n') {
            output[j++] = '\\';
            output[j++] = 'n';
        }
        else {
            output[j++] = input[i];
        }
    }
    output[j] = '\0';
    return output;
}

// Function to skip comments and whitespace at the beginning
char* skip_comments_and_whitespace(char* input) {
    char* ptr = input;

    while (*ptr) {
        // Skip leading whitespace
        while (isspace((unsigned char)*ptr)) {
            ptr++;
        }

        // Line comment
        if (ptr[0] == '-' && ptr[1] == '-') {
            // Skip until end of line
            ptr += 2;
            while (*ptr && *ptr != '\n') {
                ptr++;
            }
            continue;
        }

        // Block comment
        if (ptr[0] == '/' && ptr[1] == '*') {
            // Skip until end of comment
            ptr += 2;
            while (*ptr && !(ptr[0] == '*' && ptr[1] == '/')) {
                ptr++;
            }
            if (*ptr) {
                ptr += 2; // Skip '*/'
            }
            continue;
        }

        // Not whitespace or comment
        break;
    }

    return ptr;
}

// Hash function for table names
unsigned int hash_table_name(const char* table_name) {
    unsigned int hash = 5381;
    int c;
    while ((c = *table_name++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash % TABLE_HASH_SIZE;
}

// Get or create TableData for a table
TableData* get_or_create_table_data(const char* table_name) {
    unsigned int hash = hash_table_name(table_name);
    TableData* entry = table_hash[hash];
    while (entry) {
        if (strcmp(entry->table_name, table_name) == 0) {
            return entry; // Found existing table data
        }
        entry = entry->next;
    }
    // Create new table data
    TableData* new_entry = (TableData*)malloc(sizeof(TableData));
    new_entry->table_name = strdup(table_name);
    new_entry->head = new_entry->tail = NULL;
    new_entry->row_count = 0;
    new_entry->next = table_hash[hash];
    table_hash[hash] = new_entry;
    return new_entry;
}

// Function to process a parsed statement
void process_parsed_statement(const char* parse_tree_json) {
    cJSON* root = cJSON_Parse(parse_tree_json);
    if (!root) {
        fprintf(stderr, "Error parsing JSON\n");
        return;
    }

    cJSON* stmts = cJSON_GetObjectItem(root, "stmts");
    if (!stmts) {
        cJSON_Delete(root);
        return;
    }

    cJSON* stmt_item;
    cJSON_ArrayForEach(stmt_item, stmts) {
        cJSON* stmt = cJSON_GetObjectItem(stmt_item, "stmt");
        if (!stmt) continue;

        cJSON* insert_stmt = cJSON_GetObjectItem(stmt, "InsertStmt");
        if (insert_stmt) {
            // This is an INSERT command

            // Extract the table name
            cJSON* relation = cJSON_GetObjectItem(insert_stmt, "relation");
            if (!relation) continue;
            cJSON* schemaname = cJSON_GetObjectItem(relation, "schemaname");
            cJSON* relname = cJSON_GetObjectItem(relation, "relname");

            char full_table_name[512];
            if (schemaname && schemaname->valuestring) {
                snprintf(full_table_name, sizeof(full_table_name), "%s.%s", schemaname->valuestring, relname->valuestring);
            }
            else if (relname && relname->valuestring) {
                snprintf(full_table_name, sizeof(full_table_name), "%s", relname->valuestring);
            }
            else {
                fprintf(stderr, "Error: Table name is missing in the INSERT statement.\n");
                cJSON_Delete(root);
                return;
            }

            // Get or create the TableData for this table
            TableData* table_data = get_or_create_table_data(full_table_name);

            // Process values
            cJSON* select_stmt = cJSON_GetObjectItem(insert_stmt, "selectStmt");
            if (select_stmt) {
                cJSON* values = cJSON_GetObjectItem(select_stmt, "SelectStmt");
                if (values) {
                    cJSON* values_clause = cJSON_GetObjectItem(values, "valuesLists");

                    if (values_clause) {
                        cJSON* row_values;
                        cJSON_ArrayForEach(row_values, values_clause) {
                            // Build the data row string
                            char* data_row = build_data_row(row_values);
                            // Append the data row to the table's data list
                            append_data_row_to_table(table_data, data_row);
                        }
                    }
                }
            }
        }
        else {
            // Not an INSERT, handle as needed
        }
    }

    cJSON_Delete(root);
}

// Build a data row string from row_values
char* build_data_row(cJSON* row_values) {
    // Estimate initial buffer size
    size_t buffer_size = 1024;
    char* data_row = (char*)malloc(buffer_size);
    size_t data_row_len = 0;

    cJSON* vList = cJSON_GetObjectItem(row_values, "List");
    cJSON* vItems = cJSON_GetObjectItem(vList, "items");

    cJSON* value_item;
    int first_value = 1;
    cJSON_ArrayForEach(value_item, vItems) {
        if (!first_value) {
            if (data_row_len + 1 >= buffer_size) {
                buffer_size *= 2;
                data_row = (char*)realloc(data_row, buffer_size);
            }
            data_row[data_row_len++] = '\t';
        }
        first_value = 0;

        // Extract value as string
        const char* value_str = extract_value_as_string(value_item);

        // Ensure buffer is large enough
        size_t value_len = strlen(value_str);
        if (data_row_len + value_len + 1 >= buffer_size) {
            while (data_row_len + value_len + 1 >= buffer_size) {
                buffer_size *= 2;
            }
            data_row = (char*)realloc(data_row, buffer_size);
        }

        // Copy value into data row
        memcpy(data_row + data_row_len, value_str, value_len);
        data_row_len += value_len;
    }

    // Add newline character
    if (data_row_len + 1 >= buffer_size) {
        buffer_size += 1;
        data_row = (char*)realloc(data_row, buffer_size);
    }
    data_row[data_row_len++] = '\n';
    data_row[data_row_len] = '\0';

    return data_row;
}

// Extract value as string from value_item
const char* extract_value_as_string(cJSON* value_item) {
    static char buffer[1024];
    cJSON* val_node = value_item->child;

    if (val_node && cJSON_GetObjectItem(val_node, "A_Const")) {
        cJSON* a_Const = cJSON_GetObjectItem(val_node, "A_Const");
        cJSON* val = cJSON_GetObjectItem(a_Const, "val");

        if (val && cJSON_GetObjectItem(val, "ival")) {
            int int_val = cJSON_GetObjectItem(cJSON_GetObjectItem(val, "ival"), "ival")->valueint;
            sprintf(buffer, "%d", int_val);
            return buffer;
        }
        else if (val && cJSON_GetObjectItem(val, "fval")) {
            const char* float_str = cJSON_GetObjectItem(cJSON_GetObjectItem(val, "fval"), "fval")->valuestring;
            return float_str;
        }
        else if (val && cJSON_GetObjectItem(val, "sval")) {
            const char* str_val = cJSON_GetObjectItem(cJSON_GetObjectItem(val, "sval"), "str")->valuestring;
            // Unescape and handle special characters
            char* unescaped_str = unescape_single_quotes(str_val);
            char* escaped_str = escape_special_chars(unescaped_str);
            strncpy(buffer, escaped_str, sizeof(buffer));
            buffer[sizeof(buffer) - 1] = '\0';
            free(unescaped_str);
            free(escaped_str);
            return buffer;
        }
        else if (val && cJSON_GetObjectItem(val, "Null")) {
            return "\\N";
        }
    }
    else if (val_node && cJSON_GetObjectItem(val_node, "Null")) {
        return "\\N";
    }

    // Unsupported value type
    return "\\N";
}

// Append a data row to the table's data list
void append_data_row_to_table(TableData* table_data, char* data_row) {
    DataRow* new_row = (DataRow*)malloc(sizeof(DataRow));
    new_row->data = data_row;
    new_row->next = NULL;

    if (table_data->tail) {
        table_data->tail->next = new_row;
        table_data->tail = new_row;
    }
    else {
        table_data->head = table_data->tail = new_row;
    }

    table_data->row_count++;

    if (table_data->row_count >= MAX_BUFFERED_ROWS) {
        // Flush data for this table
        flush_table_data(stdout, table_data);
        // Reset table data
        table_data->head = table_data->tail = NULL;
        table_data->row_count = 0;
    }
}

// Flush data for a single table
void flush_table_data(FILE* output, TableData* table_data) {
    if (table_data->head == NULL) {
        return; // Nothing to flush
    }

    // Write COPY command
    fprintf(output, "COPY %s FROM stdin;\n", table_data->table_name);

    // Write data rows
    DataRow* current_row = table_data->head;
    while (current_row) {
        fprintf(output, "%s", current_row->data);
        current_row = current_row->next;
    }

    // End COPY command
    fprintf(output, "\\.\n");

    // Free data rows
    current_row = table_data->head;
    while (current_row) {
        DataRow* next_row = current_row->next;
        free(current_row->data);
        free(current_row);
        current_row = next_row;
    }

    table_data->head = table_data->tail = NULL;
    table_data->row_count = 0;
}

// Flush accumulated data for all tables
void flush_accumulated_data(FILE* output) {
    for (int i = 0; i < TABLE_HASH_SIZE; i++) {
        TableData* table_data = table_hash[i];
        while (table_data) {
            flush_table_data(output, table_data);
            table_data = table_data->next;
        }
    }
}

// Free all table data structures
void free_all_table_data() {
    for (int i = 0; i < TABLE_HASH_SIZE; i++) {
        TableData* table_data = table_hash[i];
        while (table_data) {
            free_table_data(table_data);
            table_data = table_data->next;
        }
        table_hash[i] = NULL;
    }
}

// Free a single TableData structure
void free_table_data(TableData* table_data) {
    // Free any remaining data rows
    DataRow* current_row = table_data->head;
    while (current_row) {
        DataRow* next_row = current_row->next;
        free(current_row->data);
        free(current_row);
        current_row = next_row;
    }
    free(table_data->table_name);
    free(table_data);
}

// Detect end of statement (e.g., ';' not within a string or comment)
bool detect_end_of_statement(const char* statement) {
    bool in_string = false;
    bool in_line_comment = false;
    bool in_block_comment = false;

    for (size_t i = 0; statement[i]; i++) {
        char c = statement[i];
        char next_c = statement[i + 1];

        if (in_line_comment) {
            if (c == '\n') {
                in_line_comment = false;
            }
        }
        else if (in_block_comment) {
            if (c == '*' && next_c == '/') {
                in_block_comment = false;
                i++;
            }
        }
        else if (in_string) {
            if (c == '\'') {
                if (next_c == '\'') {
                    i++; // Skip escaped quote
                }
                else {
                    in_string = false;
                }
            }
        }
        else {
            if (c == '-' && next_c == '-') {
                in_line_comment = true;
                i++;
            }
            else if (c == '/' && next_c == '*') {
                in_block_comment = true;
                i++;
            }
            else if (c == '\'') {
                in_string = true;
            }
            else if (c == ';') {
                return true; // End of statement detected
            }
        }
    }
    return false;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <input.sql> <output.sql>\n", argv[0]);
        exit(1);
    }

    FILE* input = fopen(argv[1], "r");
    if (!input) {
        perror("Error opening input file");
        exit(1);
    }

    FILE* output = freopen(argv[2], "w", stdout);
    if (!output) {
        perror("Error opening output file");
        fclose(input);
        exit(1);
    }

    char* statement_buffer = malloc(INITIAL_BUFFER_CAPACITY);
    if (!statement_buffer) {
        fprintf(stderr, "Memory allocation error\n");
        fclose(input);
        fclose(output);
        exit(1);
    }
    size_t statement_buffer_size = 0;
    size_t statement_buffer_capacity = INITIAL_BUFFER_CAPACITY;

    char line[8192];

    while (fgets(line, sizeof(line), input)) {
        size_t line_length = strlen(line);

        // Append the line to the buffer
        if (statement_buffer_size + line_length >= statement_buffer_capacity) {
            // Expand the buffer if necessary
            size_t new_capacity = statement_buffer_capacity * 2;
            while (statement_buffer_size + line_length >= new_capacity) {
                new_capacity *= 2;
            }
            char* new_buffer = realloc(statement_buffer, new_capacity);
            if (!new_buffer) {
                fprintf(stderr, "Memory allocation error\n");
                free(statement_buffer);
                fclose(input);
                fclose(output);
                exit(1);
            }
            statement_buffer = new_buffer;
            statement_buffer_capacity = new_capacity;
        }
        memcpy(statement_buffer + statement_buffer_size, line, line_length);
        statement_buffer_size += line_length;
        statement_buffer[statement_buffer_size] = '\0';

        // Detect end of statement
        if (detect_end_of_statement(statement_buffer)) {
            // We have a complete statement
            // Remove comments and whitespace
            char* statement_trimmed = skip_comments_and_whitespace(statement_buffer);

            if (strncasecmp(statement_trimmed, "INSERT INTO", 11) == 0) {
                // Parse and accumulate INSERT statement
                PgQueryParseResult result = pg_query_parse(statement_buffer);

                if (result.error) {
                    fprintf(stderr, "Warning: Error parsing SQL: %s\n", result.error->message);
                    pg_query_free_parse_result(result);

                    // Flush accumulated data before writing the problematic statement
                    flush_accumulated_data(output);

                    // Write the problematic statement as is
                    fprintf(output, "%s", statement_buffer);
                }
                else {
                    // Accumulate data
                    process_parsed_statement(result.parse_tree);
                    pg_query_free_parse_result(result);
                }
            }
            else {
                // Flush accumulated data before handling non-INSERT statement
                flush_accumulated_data(output);

                // Write non-INSERT statement to output
                fprintf(output, "%s", statement_buffer);
            }

            // Reset statement buffer
            statement_buffer_size = 0;
            statement_buffer[0] = '\0';
        }
    }

    // Process any remaining statement
    if (statement_buffer_size > 0) {
        // Remove comments and whitespace
        char* statement_trimmed = skip_comments_and_whitespace(statement_buffer);

        if (strncasecmp(statement_trimmed, "INSERT INTO", 11) == 0) {
            // Parse and accumulate INSERT statement
            PgQueryParseResult result = pg_query_parse(statement_buffer);

            if (result.error) {
                fprintf(stderr, "Warning: Error parsing SQL: %s\n", result.error->message);
                pg_query_free_parse_result(result);

                // Flush accumulated data before writing the problematic statement
                flush_accumulated_data(output);

                // Write the problematic statement as is
                fprintf(output, "%s", statement_buffer);
            }
            else {
                // Accumulate data
                process_parsed_statement(result.parse_tree);
                pg_query_free_parse_result(result);
            }
        }
        else {
            // Flush accumulated data before handling non-INSERT statement
            flush_accumulated_data(output);

            // Write non-INSERT statement to output
            fprintf(output, "%s", statement_buffer);
        }
    }

    // Flush any remaining accumulated data
    flush_accumulated_data(output);

    // Free resources
    free_all_table_data();
    free(statement_buffer);
    fclose(input);
    fclose(output);

    return 0;
}
