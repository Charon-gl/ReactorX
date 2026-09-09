cmake_minimum_required(VERSION 3.15.1)
project(ReactorX)

# 编译标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# 源文件
file(GLOB_RECURSE SRC ${PROJECT_SOURCE_DIR}/src/*.cpp)

add_subdirectory(llhttp)
add_executable(Reactor ${SRC})

target_link_libraries(Reactor PRIVATE llhttp)
target_include_directories(Reactor PRIVATE ${PROJECT_SOURCE_DIR}/include)
