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
find_library(JEMALLOC_LIBRARY
    NAMES jemalloc
    HINTS /usr/local/lib /usr/lib
)
if(NOT JEMALLOC_LIBRARY)
    message(FATAL_ERROR "jemalloc not found. Please install jemalloc or specify its path.")
endif()

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

# 添加可执行文件
add_executable(kvstore
    kvstore.cpp

    network/proactor.cpp
    network/reactor.cpp
    network/ntyco.c          # 纯 C 源文件

    storage/kvs_array.cpp
    storage/kvs_rbtree.cpp
    storage/kvs_hash.cpp
    storage/kvs_skiptable.cpp

    persistence/snapshot.cpp
    persistence/aof.cpp

    replication/kvs_replication.cpp
)

# 包含目录
target_include_directories(kvstore PRIVATE
    ${NTYCO_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/include
)

# 链接目录（用于 NtyCo）
target_link_directories(kvstore PRIVATE ${NTYCO_LIBRARY_DIR})

# 链接库
target_link_libraries(kvstore
    ${URING_LIBRARY}
    ${JEMALLOC_LIBRARY}
    ntyco                    # 库名，依赖于前面的 link_directories
    Threads::Threads         # 提供 pthread
    ${CMAKE_DL_LIBS}         # 提供 dl（如果系统需要）
)

# # 如果使用 GCC/Clang，建议开启常用警告（可选）
# if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
#     target_compile_options(kvstore PRIVATE -Wall -Wextra)
# endif()