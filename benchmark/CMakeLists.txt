# benchmark/CMakeLists.txt
#
# 依赖：
#   - 框架库 ReactorX（由根 CMakeLists.txt 编译，见根目录）
#   - 第三方库 llhttp（HTTP 解析，位于 src/llhttp/）

# 调试
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# HTTP 演示源码：入口 main.cpp + 协议处理 http_handler.cpp
set(APP_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/app/main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/http_handler.cpp
)

add_executable(Reactor ${APP_SRC})

target_include_directories(Reactor PRIVATE
    ${PROJECT_SOURCE_DIR}/include                       # 框架头文件（core/net/reactor/utils）
    ${CMAKE_CURRENT_SOURCE_DIR}/src                     # http_handler.hpp / response_data.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/llhttp/include      # llhttp.h
)

# llhttp 静态库（避免运行时依赖 .so，二进制可独立分发）
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON  CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/llhttp llhttp_build)

target_link_libraries(Reactor PRIVATE ReactorX llhttp::llhttp)
