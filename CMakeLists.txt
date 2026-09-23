# 指定 CMake 最低版本要求
cmake_minimum_required(VERSION 3.10)

# 设置项目名称和语言（C 和 C++）
project(kvstore LANGUAGES C CXX)

# 设置 C++ 标准（C++11 已足够）
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)   # 推荐关闭 GNU 扩展，提高可移植性

# 查找线程库（必须）
find_package(Threads REQUIRED)

# 允许用户通过 -D 覆盖 NtyCo 根目录
set(NTYCO_ROOT "${CMAKE_SOURCE_DIR}/NtyCo-master" CACHE PATH "Path to NtyCo library root")
set(NTYCO_INCLUDE_DIR "${NTYCO_ROOT}/core" CACHE PATH "Path to NtyCo include directory")
set(NTYCO_LIBRARY_DIR "${NTYCO_ROOT}" CACHE PATH "Path to NtyCo library directory")

# 查找 liburing
find_library(URING_LIBRARY
    NAMES uring
    HINTS /usr/local/lib /usr/lib
)
if(NOT URING_LIBRARY)
    message(FATAL_ERROR "liburing not found. Please install liburing or specify its path.")
endif()

# 查找 jemalloc
# find_library(JEMALLOC_LIBRARY
#     NAMES jemalloc
#     HINTS /usr/local/lib /usr/lib
# )
# if(NOT JEMALLOC_LIBRARY)
#     message(FATAL_ERROR "jemalloc not found. Please install jemalloc or specify its path.")
# endif()

# 可选：检查 liburing 头文件
find_path(URING_INCLUDE_DIR
    NAMES liburing.h
    HINTS /usr/local/include /usr/include
)
if(URING_INCLUDE_DIR)
    include_directories(${URING_INCLUDE_DIR})
else()
    message(WARNING "liburing header not found in standard paths. Building may fail.")
endif()

# 查找 RDMA 相关依赖。未安装开发头文件或运行库时自动回退到纯 TCP 全量同步。
find_path(RDMA_VERBS_INCLUDE_DIR
    NAMES infiniband/verbs.h
    HINTS /usr/local/include /usr/include
)
find_path(RDMA_CMA_INCLUDE_DIR
    NAMES rdma/rdma_cma.h
    HINTS /usr/local/include /usr/include
)
find_library(IBVERBS_LIBRARY
    NAMES ibverbs
    HINTS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
)
find_library(RDMACM_LIBRARY
    NAMES rdmacm
    HINTS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
)

set(KVSTORE_RDMA_FOUND FALSE)
if(RDMA_VERBS_INCLUDE_DIR AND RDMA_CMA_INCLUDE_DIR AND IBVERBS_LIBRARY AND RDMACM_LIBRARY)
    set(KVSTORE_RDMA_FOUND TRUE)
else()
    message(WARNING "RDMA development headers/libraries not found. Building without RDMA full sync; TCP fallback will be used.")
endif()

set(KVSTORE_SOURCES
    kvstore.cpp

    config/kvs_config.cpp

    network/proactor.cpp
    network/reactor.cpp
    network/ntyco.c

    storage/kvs_array.cpp
    storage/kvs_rbtree.cpp
    storage/kvs_hash.cpp
    storage/kvs_skiptable.cpp

    memorypool/memorypool.cpp

    persistence/snapshot.cpp
    persistence/aof.cpp
    persistence/io_uring.cpp

    replication/kvs_replication.cpp
    replication/kvs_ebpf.cpp
)

if(KVSTORE_RDMA_FOUND)
    list(APPEND KVSTORE_SOURCES replication/kvs_rdma.cpp)
endif()

# 添加可执行文件
add_executable(kvstore ${KVSTORE_SOURCES})

# 持久化循环脚本使用的测试客户端
add_executable(rdb_save testcase/RDB/save.cpp)
add_executable(rdb_load testcase/RDB/load.cpp)
add_executable(aof_save testcase/AOF/save.cpp)
add_executable(aof_load testcase/AOF/load.cpp)
add_executable(get10w testcase/master-replica/get.cpp)
add_executable(insert_after5w testcase/master-replica/insert_after5w.cpp)
add_executable(insert_before5w testcase/master-replica/insert_before5w.cpp)
add_executable(test testcase/test/testcase.cpp)

set_target_properties(rdb_save PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/RDB" OUTPUT_NAME save)
set_target_properties(rdb_load PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/RDB" OUTPUT_NAME load)
set_target_properties(aof_save PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/AOF" OUTPUT_NAME save)
set_target_properties(aof_load PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/AOF" OUTPUT_NAME load)
set_target_properties(get10w insert_after5w insert_before5w
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/master-replica"
)
set_target_properties(test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/test")

foreach(testcase_target rdb_save rdb_load aof_save aof_load
        get10w insert_after5w insert_before5w test)
    target_include_directories(${testcase_target} PRIVATE
        ${CMAKE_SOURCE_DIR}/testcase
    )
endforeach()

target_link_libraries(test Threads::Threads)

# 包含目录
target_include_directories(kvstore PRIVATE
    ${NTYCO_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/include
)

if(KVSTORE_RDMA_FOUND)
    target_include_directories(kvstore PRIVATE
        ${RDMA_VERBS_INCLUDE_DIR}
        ${RDMA_CMA_INCLUDE_DIR}
    )
    target_compile_definitions(kvstore PRIVATE KVS_ENABLE_RDMA=1)
endif()

# 链接目录（用于 NtyCo）
target_link_directories(kvstore PRIVATE ${NTYCO_LIBRARY_DIR})

# 链接库
target_link_libraries(kvstore
    ${URING_LIBRARY}
    # ${JEMALLOC_LIBRARY}
    ntyco                    # 库名，依赖于前面的 link_directories
    Threads::Threads         # 提供 pthread
    ${CMAKE_DL_LIBS}         # 提供 dl（如果系统需要）
)

if(KVSTORE_RDMA_FOUND)
    target_link_libraries(kvstore
        ${RDMACM_LIBRARY}
        ${IBVERBS_LIBRARY}
    )
endif()
