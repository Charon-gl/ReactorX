cmake_minimum_required(VERSION 3.15.1)
project(ReactorX)

# 编译标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 调试
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# 框架源文件
file(GLOB_RECURSE FRAMEWORK_SRC CMAKE_CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/src/*.cpp)

# 编译成静态库
add_library(ReactorX STATIC ${FRAMEWORK_SRC})

target_include_directories(ReactorX PUBLIC ${PROJECT_SOURCE_DIR}/include)

# 若存在 benchmark 目录，额外编译
if(EXISTS ${PROJECT_SOURCE_DIR}/benchmark/CMakeLists.txt)
    add_subdirectory(benchmark)
endif()
