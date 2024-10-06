// pg_dump.exe -U dart --column-inserts -f arquivo_input.sql teste
// .\build\Release\insert_to_copy.exe arquivo_input.sql arquivo_output.sql


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

// Função para substituir aspas simples escapadas por uma única aspa simples
char* unescape_single_quotes(const char* input) {
    size_t len = strlen(input);
    char* output = malloc(len + 1);
    if (!output) {
        fprintf(stderr, "Erro de alocação de memória\n");
        exit(1);
    }
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (input[i] == '\'' && input[i + 1] == '\'') {
            output[j++] = '\'';
            i++; // Pular a segunda aspa
        }
        else {
            output[j++] = input[i];
        }
    }
    output[j] = '\0';
    return output;
}

// Função para ignorar comentários e espaços em branco no início do buffer
char* skip_comments_and_whitespace(char* input) {
    char* ptr = input;

    while (*ptr) {
        // Ignorar espaços em branco iniciais
        while (isspace((unsigned char)*ptr)) {
            ptr++;
        }

        // Verificar se é um comentário de linha
        if (ptr[0] == '-' && ptr[1] == '-') {
            // Pular até o final da linha ou do buffer
            ptr += 2;
            while (*ptr && *ptr != '\n') {
                ptr++;
            }
            // Se chegar ao fim do buffer, sair
            if (!*ptr) {
                break;
            }
            ptr++; // Pular o '\n'
            continue;
        }

        // Verificar se é um comentário de bloco
        if (ptr[0] == '/' && ptr[1] == '*') {
            // Pular até o final do comentário de bloco
            ptr += 2;
            while (*ptr && !(ptr[0] == '*' && ptr[1] == '/')) {
                ptr++;
            }
            // Se chegar ao fim do buffer, sair
            if (!*ptr) {
                break;
            }
            ptr += 2; // Pular '*/'
            continue;
        }

        // Se não for espaço em branco nem comentário, sair do loop
        break;
    }

    return ptr;
}

// Função para processar uma instrução analisada
void process_parsed_statement(const char* parse_tree_json, FILE* output) {
    // Analisar o JSON
    cJSON* root = cJSON_Parse(parse_tree_json);
    if (!root) {
        fprintf(stderr, "Erro ao analisar o JSON\n");
        return;
    }

    //printf("JSON: %s",parse_tree_json);

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
            // Este é um comando INSERT

            // Extrair o nome da tabela
            cJSON* relation = cJSON_GetObjectItem(insert_stmt, "relation");
            if (!relation) continue;
            cJSON* schemaname = cJSON_GetObjectItem(relation, "schemaname");
            cJSON* relname = cJSON_GetObjectItem(relation, "relname");

            char full_table_name[512];
            if (schemaname && schemaname->valuestring) {
                snprintf(full_table_name, sizeof(full_table_name), "%s.%s", schemaname->valuestring, relname->valuestring);
            }
            else {
                snprintf(full_table_name, sizeof(full_table_name), "%s", relname->valuestring);
            }

            // Iniciar o comando COPY
            fprintf(output, "COPY %s FROM stdin;\n", full_table_name);

            // Processar valores
            cJSON* cols = cJSON_GetObjectItem(insert_stmt, "cols");
            cJSON* colnames = NULL;
            if (cols) {
                colnames = cJSON_CreateArray();
                cJSON* col_item;
                cJSON_ArrayForEach(col_item, cols) {
                    cJSON* res_target = cJSON_GetObjectItem(col_item, "ResTarget");
                    if (res_target) {
                        cJSON* name = cJSON_GetObjectItem(res_target, "name");
                        
                        if (name) {
                            cJSON_AddItemToArray(colnames, cJSON_Duplicate(name, 1));
                        }
                    }
                }
            }

            cJSON* values_lists = cJSON_GetObjectItem(insert_stmt, "selectStmt");
            if (values_lists) {
                cJSON* values = cJSON_GetObjectItem(values_lists, "SelectStmt");
                if (values) {
                    cJSON* values_clause = cJSON_GetObjectItem(values, "valuesLists");
                    
                    if (values_clause) {
                        cJSON* row_values;
                        cJSON_ArrayForEach(row_values, values_clause) {
                            cJSON* vList = cJSON_GetObjectItem(row_values, "List");
                            cJSON* vItems = cJSON_GetObjectItem(vList, "items");
                            // Processar cada valor da linha
                            cJSON* value_item;
                            int first_value = 1;
                            cJSON_ArrayForEach(value_item, vItems) {
                                if (!first_value) {
                                    fputc('\t', output);
                                }
                                first_value = 0;

                                cJSON* val_node = value_item->child;
                                if (!val_node) {
                                    fputs("\\N", output);
                                    continue;
                                }
                                cJSON* a_Const = cJSON_GetObjectItem(value_item,"A_Const");
                                if (a_Const) {                                    
                                       
                                      if (cJSON_GetObjectItem(a_Const, "ival")) {
                                            cJSON* ival = cJSON_GetObjectItem(a_Const, "ival");
                                            int int_val = cJSON_GetObjectItem(ival, "ival")->valueint;
                                            fprintf(output, "%d", int_val);
                                       }                                        
                                       else if (cJSON_GetObjectItem(a_Const, "fval")) {
                                            cJSON* fval = cJSON_GetObjectItem(a_Const, "fval");
                                            const char* float_val = cJSON_GetObjectItem(fval, "fval")->valuestring;
                                            fprintf(output, "%s", float_val);
                                        }
                                        else if (cJSON_GetObjectItem(a_Const, "sval")) {
                                            cJSON* sval = cJSON_GetObjectItem(a_Const, "sval");
                                            const char* str_val = cJSON_GetObjectItem(sval, "sval")->valuestring;
                                            // Desfazer escape de aspas simples
                                            char* unescaped_str = unescape_single_quotes(str_val);
                                            // Escapar tabulações e quebras de linha
                                            for (char* p = unescaped_str; *p; p++) {
                                                if (*p == '\t') {
                                                    fputs("\\t", output);
                                                }
                                                else if (*p == '\n') {
                                                    fputs("\\n", output);
                                                }
                                                else {
                                                    fputc(*p, output);
                                                }
                                            }
                                            free(unescaped_str);
                                        }
                                        else if (cJSON_GetObjectItem(a_Const, "Null")) {
                                            fputs("\\N", output);
                                        }
                                        else {
                                            // Tipo de valor não suportado
                                            fputs("\\N", output);
                                        }
                                    
                                }
                                else if (strcmp(val_node->string, "Null") == 0) {
                                    fputs("\\N", output);
                                }
                                else {
                                    // Tipo de nó não suportado
                                    fputs("\\N", output);
                                }
                            }
                            // Fim da linha
                            fputc('\n', output);
                        }
                    }
                }
            }

            // Finalizar o comando COPY
            fprintf(output, "\\.\n");

            if (colnames) {
                cJSON_Delete(colnames);
            }
        }
        else {
            // Não é um INSERT, pode ser processado conforme necessário
        }
    }

    cJSON_Delete(root);
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s <input.sql> <output.sql>\n", argv[0]);
        exit(1);
    }

    FILE* input = fopen(argv[1], "r");
    if (!input) {
        perror("Erro ao abrir o arquivo de entrada");
        exit(1);
    }

    FILE* output = fopen(argv[2], "w");
    if (!output) {
        perror("Erro ao abrir o arquivo de saída");
        fclose(input);
        exit(1);
    }

    char* statement_buffer = malloc(INITIAL_BUFFER_CAPACITY);
    if (!statement_buffer) {
        fprintf(stderr, "Erro de alocação de memória\n");
        fclose(input);
        fclose(output);
        exit(1);
    }
    size_t statement_buffer_size = 0;
    size_t statement_buffer_capacity = INITIAL_BUFFER_CAPACITY;

    bool in_string = false;
    bool in_line_comment = false;
    bool in_block_comment = false;

    char line[8192];

    while (fgets(line, sizeof(line), input)) {
        size_t line_length = strlen(line);

        // Anexar a linha ao buffer
        if (statement_buffer_size + line_length >= statement_buffer_capacity) {
            // Expandir o buffer se necessário
            size_t new_capacity = statement_buffer_capacity * 2;
            while (statement_buffer_size + line_length >= new_capacity) {
                new_capacity *= 2;
            }
            char* new_buffer = realloc(statement_buffer, new_capacity);
            if (!new_buffer) {
                fprintf(stderr, "Erro de alocação de memória\n");
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

        // Atualizar o estado de acordo com o conteúdo da linha
        for (size_t i = 0; i < line_length; i++) {
            char c = line[i];
            char next_c = (i + 1 < line_length) ? line[i + 1] : '\0';

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
                        i++; // Pular aspas escapadas
                    }
                    else {
                        in_string = false;
                    }
                }
                continue; // Continuar dentro da string
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
                    // Fim da instrução

                    // Temos uma instrução completa
                    // Remover comentários e espaços em branco iniciais
                    char* statement_trimmed = skip_comments_and_whitespace(statement_buffer);

                    if (strncasecmp(statement_trimmed, "INSERT INTO", 11) == 0) {
                        // É um INSERT, tentar analisar e converter
                        PgQueryParseResult result = pg_query_parse(statement_buffer);

                        if (result.error) {
                            fprintf(stderr, "Aviso: Erro ao analisar SQL: %s\n", result.error->message);
                            pg_query_free_parse_result(result);
                            // Escrever a instrução original no arquivo de saída
                            fprintf(output, "%s", statement_buffer);
                        }
                        else {
                            // Processar o resultado analisado
                            process_parsed_statement(result.parse_tree, output);
                            pg_query_free_parse_result(result);
                        }
                    }
                    else {
                        // Não é um INSERT, escrever a instrução original no arquivo de saída
                        fprintf(output, "%s", statement_buffer);
                    }

                    // Ajustar o índice para continuar após o ponto e vírgula
                    i++; // Avançar para o próximo caractere
                    if (i < line_length) {
                        // Processar o restante da linha
                        statement_buffer_size = 0;
                        statement_buffer[0] = '\0';

                        // Copiar o restante da linha para o buffer de instrução
                        size_t remaining_length = line_length - i;
                        memcpy(statement_buffer, line + i, remaining_length);
                        statement_buffer_size = remaining_length;
                        statement_buffer[statement_buffer_size] = '\0';

                        // Como já atualizamos o índice, continuamos o loop
                        continue;
                    }
                    else {
                        // Reiniciar o buffer
                        statement_buffer_size = 0;
                        statement_buffer[0] = '\0';
                        break;
                    }
                }
            }
        }
    }

    // Processar qualquer instrução restante
    if (statement_buffer_size > 0) {
        char* statement_trimmed = skip_comments_and_whitespace(statement_buffer);

        if (strncasecmp(statement_trimmed, "INSERT INTO", 11) == 0) {
            PgQueryParseResult result = pg_query_parse(statement_buffer);

            if (result.error) {
                fprintf(stderr, "Aviso: Erro ao analisar SQL: %s\n", result.error->message);
                pg_query_free_parse_result(result);
                fprintf(output, "%s", statement_buffer);
            }
            else {
                process_parsed_statement(result.parse_tree, output);
                pg_query_free_parse_result(result);
            }
        }
        else {
            fprintf(output, "%s", statement_buffer);
        }
    }

    free(statement_buffer);
    fclose(input);
    fclose(output);

    return 0;
}
