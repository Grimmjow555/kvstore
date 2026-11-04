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
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

# 允许用户通过 -D 覆盖 NtyCo 根目录（submodule 目录为 NtyCo）
set(NTYCO_ROOT "${CMAKE_SOURCE_DIR}/NtyCo" CACHE PATH "Path to NtyCo library root")
set(NTYCO_INCLUDE_DIR "${NTYCO_ROOT}/core" CACHE PATH "Path to NtyCo include directory")

if(NOT EXISTS "${NTYCO_INCLUDE_DIR}/nty_coroutine.h")
    message(FATAL_ERROR
        "未找到 NtyCo 源码: ${NTYCO_INCLUDE_DIR}/nty_coroutine.h\n"
        "请先初始化子模块: git submodule sync --recursive && git submodule update --init --recursive")
endif()

# NtyCo 的 libntyco.a 不在版本库中（objs/、lib/ 都在 submodule 的 .gitignore 里），
# 依赖它会出现「手工构建一次后又丢失、链接报 cannot find -lntyco」的问题。
# 这里直接把 NtyCo 源码编进 kvstore，不再依赖 submodule 内的构建产物。
set(NTYCO_SOURCES
    ${NTYCO_ROOT}/core/nty_coroutine.c
    ${NTYCO_ROOT}/core/nty_epoll.c
    ${NTYCO_ROOT}/core/nty_schedule.c
    ${NTYCO_ROOT}/core/nty_socket.c
)

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
    ${NTYCO_SOURCES}

    storage/kvs_array.cpp
    storage/kvs_rbtree.cpp
    storage/kvs_hash.cpp
    storage/kvs_skiptable.cpp

    memorypool/memorypool.cpp

    persistence/snapshot.cpp
    persistence/aof.cpp
    persistence/io_uring.cpp

    replication/kvs_replication.cpp
    replication/kvs_ebpf_ring.cpp
    replication/kvs_sync.cpp
    replication/kvs_netutil.cpp
)

add_library(kvs_integrity STATIC persistence/integrity.cpp)
target_include_directories(kvs_integrity PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(kvs_integrity PUBLIC OpenSSL::Crypto)

# 添加可执行文件
add_executable(kvstore ${KVSTORE_SOURCES})

# 阶段 3：增量采集与转发拆成独立进程 ebpf-agent（uprobe 挂 master 二进制，
# 或 sim 模式读 master 创建的共享内存环）。产地在 build/ 根目录。
add_executable(ebpf_agent
    tools/ebpf_agent.cpp
    replication/kvs_ebpf_agent.cpp
    replication/kvs_ebpf_ring.cpp
    replication/kvs_netutil.cpp
    config/kvs_config.cpp
    config/kvs_agent_config.cpp
)
target_include_directories(ebpf_agent PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(ebpf_agent Threads::Threads)
set_target_properties(ebpf_agent PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}" OUTPUT_NAME "ebpf-agent")

add_executable(agent_config_test
    testcase/ebpf-agent/test_agent_config.cpp
    config/kvs_agent_config.cpp
    config/kvs_config.cpp
)
target_include_directories(agent_config_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(agent_config_test Threads::Threads)
set_target_properties(agent_config_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/ebpf-agent")

add_executable(agent_startup_test
    testcase/ebpf-agent/test_agent_startup.cpp
    replication/kvs_ebpf_ring.cpp
    replication/kvs_netutil.cpp
)
target_include_directories(agent_startup_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_definitions(agent_startup_test PRIVATE
    KVS_AGENT_BINARY="$<TARGET_FILE:ebpf_agent>")
add_dependencies(agent_startup_test ebpf_agent)
set_target_properties(agent_startup_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/ebpf-agent")

# 阶段 2：RDMA 全量搬运拆成两个独立进程。它们与 kvstore 分开构建，
# 由 Master / Replica 在 SYNC 流程中按需派生（见 docs/replication-redesign.md）。
# 产物直接放在 build/ 根目录，便于用默认的 ./rdma-client、./rdma-server 启动。
if(KVSTORE_RDMA_FOUND)
    add_executable(rdma_client tools/rdma_client.cpp replication/kvs_rdma_transfer.cpp)
    add_executable(rdma_server tools/rdma_server.cpp replication/kvs_rdma_transfer.cpp)
    foreach(rdma_tool rdma_client rdma_server)
        target_include_directories(${rdma_tool} PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${RDMA_VERBS_INCLUDE_DIR}
            ${RDMA_CMA_INCLUDE_DIR}
        )
        target_link_libraries(${rdma_tool}
            ${RDMACM_LIBRARY}
            ${IBVERBS_LIBRARY}
        )
    endforeach()
    set_target_properties(rdma_client PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}" OUTPUT_NAME "rdma-client")
    set_target_properties(rdma_server PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}" OUTPUT_NAME "rdma-server")
endif()

# 持久化循环脚本使用的测试客户端
add_executable(rdb_save testcase/RDB/save.cpp)
add_executable(rdb_load testcase/RDB/load.cpp)
add_executable(aof_save testcase/AOF/save.cpp)
add_executable(aof_load testcase/AOF/load.cpp)
add_executable(get10w testcase/master-replica/get10w.cpp)
add_executable(insert_after5w testcase/master-replica/insert_after5w.cpp)
add_executable(insert_before5w testcase/master-replica/insert_before5w.cpp)
add_executable(test testcase/test/testcase.cpp)
add_executable(memptest testcase/memorypool/memptest.cpp)
add_executable(memptest_pthread testcase/memorypool/memptest_pthread.cpp)
add_executable(integrity_test testcase/integrity/test_integrity.cpp)
add_executable(integrity_client testcase/integrity/client.cpp)
add_executable(batchsend testcase/batchsend/batchsend.cpp)
add_executable(longdata testcase/longdata/longdata.cpp)
add_executable(echo_save testcase/ECHO/echo.cpp)

set_target_properties(rdb_save PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/RDB" OUTPUT_NAME save)
set_target_properties(rdb_load PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/RDB" OUTPUT_NAME load)
set_target_properties(aof_save PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/AOF" OUTPUT_NAME save)
set_target_properties(aof_load PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/AOF" OUTPUT_NAME load)
set_target_properties(get10w insert_after5w insert_before5w
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/master-replica"
)
set_target_properties(test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/test")
set_target_properties(memptest memptest_pthread
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/memorypool"
)
set_target_properties(integrity_test
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/integrity"
)
set_target_properties(integrity_client
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/integrity"
)
set_target_properties(batchsend
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/batchsend"
)
set_target_properties(longdata
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/longdata"
)
set_target_properties(echo_save
    PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/testcase/ECHO" OUTPUT_NAME ECHO
)

target_include_directories(integrity_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(integrity_test OpenSSL::Crypto)
target_link_libraries(integrity_test kvs_integrity)

target_link_libraries(test Threads::Threads)
target_link_libraries(memptest Threads::Threads)
target_link_libraries(memptest_pthread Threads::Threads)

# 包含目录
target_include_directories(kvstore PRIVATE
    ${NTYCO_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/include
)

# 链接库
target_link_libraries(kvstore
    ${URING_LIBRARY}
    kvs_integrity
    # ${JEMALLOC_LIBRARY}
    # NtyCo 源码已随 KVSTORE_SOURCES 一起编译，无需再链接 libntyco.a
    Threads::Threads         # 提供 pthread
    ${CMAKE_DL_LIBS}         # 提供 dl（如果系统需要）
)
