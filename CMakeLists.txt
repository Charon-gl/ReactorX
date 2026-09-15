cmake_minimum_required(VERSION 3.15.1)
project(ReactorX)

# 编译标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 调试
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# 源文件
file(GLOB_RECURSE FRAMEWORK_SRC CMAKE_CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE PROTOCOL_SRC  CMAKE_CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/protocol/*.cpp)
file(GLOB_RECURSE APP_SRC       CMAKE_CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/app/*.cpp)

add_executable(Reactor ${FRAMEWORK_SRC} ${PROTOCOL_SRC} ${APP_SRC})

# 头文件
target_include_directories(Reactor PRIVATE 
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/protocol
    ${PROJECT_SOURCE_DIR}/app
    ${PROJECT_SOURCE_DIR}/llhttp/include
)

# llhttp 静态库
set(LLHTTP_BUILD_STATIC ON CACHE BOOL "" FORCE)
add_subdirectory(llhttp)
target_link_libraries(Reactor PRIVATE llhttp::llhttp)
