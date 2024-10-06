# mkdir build
# cd build
# cmake -G "Visual Studio 17 2022" ..
# cmake --build . --config Release
cmake_minimum_required(VERSION 3.10)
project(pg_query C)

# Use file(GLOB) para buscar arquivos .c
file(GLOB SRC_FILES
    "src/*.c"
    "src/postgres/*.c"
    "vendor/protobuf-c/protobuf-c.c"
    "vendor/xxhash/xxhash.c"
    "protobuf/pg_query.pb-c.c"
    "protobuf/pg_query.pb.cc"  # Adicionado se necessário
)

# Defina os diretórios de inclusão
include_directories(
    .
    ./vendor
    ./src/postgres/include
    ./src/include
    ./src/postgres/include/port/win32
    ./src/postgres/include/port/win32_msvc
    ./protobuf            # Adicionado para incluir arquivos de cabeçalho do Protobuf
    ./cjson            # Adicionado para incluir cJSON.h
    ./examples
)

# Crie a biblioteca estática pg_query
add_library(pg_query STATIC ${SRC_FILES})
#add_library(pg_query SHARED ${SRC_FILES})

# Defina as flags de compilação
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I. -I./vendor -I./src/postgres/include -I./src/include -I./src/postgres/include/port/win32 -I./src/postgres/include/port/win32_msvc -I./protobuf -I./cjson")


#Defina os executáveis de exemplo
set(EXAMPLES    
    simple
    # scan
    # normalize
    # simple_error
    # normalize_error
    # simple_plpgsql
)

foreach(example ${EXAMPLES})
    add_executable(${example} examples/${example}.c)
    target_link_libraries(${example} pg_query)
endforeach()

# Adicione os arquivos fonte
set(EXAMPLES
    ${PROJECT_SOURCE_DIR}/examples/insert_to_copy.c
    ${PROJECT_SOURCE_DIR}/cjson/cJSON.c
)
add_executable(insert_to_copy ${EXAMPLES})
target_link_libraries(insert_to_copy pg_query)

# Defina os executáveis de teste com nomes simples
# set(TESTS
#     deparse
#     fingerprint
#     fingerprint_opts
#     normalize_test
#     parse
#     parse_opts
#     parse_protobuf
#     parse_protobuf_opts
#     parse_plpgsql
#     scan_test
#     split
# )

# foreach(test ${TESTS})
#     add_executable(${test} test/${test}.c test/${test}_tests.c)
#     target_link_libraries(${test} pg_query)
# endforeach()

# Limpar os arquivos gerados 
# add_custom_target(clean_files
#     COMMAND ${CMAKE_COMMAND} -E remove *.obj pg_query.lib
# )

# Alvo de instalação (opcional)
install(TARGETS pg_query DESTINATION lib)
install(FILES ${SRC_FILES} DESTINATION include/pg_query)