# 指定 CMake 最低版本要求
cmake_minimum_required(VERSION 3.10)

# 设置项目名称和语言（C 和 C++）
project(kvstore LANGUAGES C CXX)

# 设置 C++ 标准（如果用到 C++11/14/17 特性，这里可以调整）
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)



# 添加需要编译的源文件
# 注意：C 源文件和 C++ 源文件要分开列出，CMake 会自动选择合适的编译器
add_executable(kvstore
    kvstore.cpp

    proactor.cpp
    reactor.cpp
    ntyco.c          # 纯 C 源文件

    kvs_array.cpp
    rbtree.cpp
)

# 指定头文件搜索路径（相当于之前的 -I NtyCo-master/core/）
target_include_directories(kvstore PRIVATE ${CMAKE_SOURCE_DIR}/NtyCo-master/core/)


target_link_directories(kvstore PRIVATE ${CMAKE_SOURCE_DIR}/NtyCo-master)
target_link_libraries(kvstore
    uring
    ntyco
)