cmake_minimum_required(VERSION 3.15.1)
project(ReactorX)

# 编译标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 调试
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# 框架源文件：只编译框架本身（core/net/reactor/utils），不含任何 demo/HTTP/业务代码
file(GLOB_RECURSE FRAMEWORK_SRC CMAKE_CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/src/*.cpp)

# 框架编译成静态库，供上层业务链接使用
add_library(ReactorX STATIC ${FRAMEWORK_SRC})

target_include_directories(ReactorX PUBLIC ${PROJECT_SOURCE_DIR}/include)

# 若存在 benchmark 目录（仅测试/演示分支），额外编译 HTTP 演示服务器
if(EXISTS ${PROJECT_SOURCE_DIR}/benchmark/CMakeLists.txt)
    add_subdirectory(benchmark)
endif()
