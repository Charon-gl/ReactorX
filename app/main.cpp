#include "reactor/MainReactor.hpp"
#include "net/FrameWorkDispatcher.hpp"
#include "core/ReactorEvent.hpp"

/*
异步模式的框架已经写好了，下一步是先把框架改成同步模式，对同步模式进行压测，然后才是异步模式压测，去观察跨线程开销的代价，以及如何性能优化
*/

class app_server : public IRequestSink
{
private:
    std::unique_ptr<FrameWorkDispatcher> dispatcher;
    std::unique_ptr<MainReactor> reactor;
public:
    app_server(uint16_t port, int core)
        : dispatcher(std::make_unique<FrameWorkDispatcher>(*this)),
          reactor(std::make_unique<MainReactor>(port, core, *dispatcher)) {}

    void submit(Request request) override
    {
        constexpr char response[] =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 11\r\n"
            "Connection: close\r\n"
            "\r\n"
            "hello test\n";

        dispatcher->submit_command(Reactor_Command{
            Response_Command{request.token, response}
        });
    }

    void run() { reactor->run(); }
};


int main(int argc, char** argv)
{
    // http_server server(static_cast<uint16_t>(6666), 1);
    // server.run();
    
    app_server server(6666, 2);
    server.run();

    return 0;

}