// echo_server.cpp —— 纯 TCP 回显服务器（基于 ReactorX，不依赖 HTTP）
// 收到多少字节就原样回显多少字节，用于测量框架的真实往返延迟与吞吐，
// 摆脱 wrk2 的 coordinated omission 修正带来的延迟虚高。
#include "reactor/MainReactor.hpp"
#include "net/Dispatcher.hpp"
#include "core/ReactorEvent.hpp"
#include <cstdlib>

int main(int argc, char* argv[])
{
    uint16_t port    = argc > 1 ? (uint16_t)std::atoi(argv[1]) : 6666;
    int      sub_num = argc > 2 ? std::atoi(argv[2]) : 4;

    Dispatcher dispatcher;

    // 纯 TCP 回显：收到一段字节流，原样投回框架发送
    dispatcher.set_business_callback([&dispatcher](ByteStream stream) {
        dispatcher.submit_operation(
            Operation{Send_Operation{stream.token, std::move(stream.data)}});
    });

    MainReactor reactor(port, sub_num, dispatcher);
    reactor.run();
    return 0;
}
