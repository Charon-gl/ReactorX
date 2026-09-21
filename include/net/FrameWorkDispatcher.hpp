#pragma once 
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include "core/ReactorEvent.hpp"


class IRequestSink      // 上层业务的基类，也是与框架层的接口
{
public:
    virtual void submit(Request task) = 0;
    virtual ~IRequestSink() = default;
};

struct port
{
    std::mutex mtx;
    std::function<void(Reactor_Command)> submit_command;
    bool active;

    port(std::function<void(Reactor_Command)> _cb) : active(true) { submit_command = std::move(_cb); }
};

class FrameWorkDispatcher
{
public:
    using Reactor_Ports = std::unordered_map<uint32_t, std::unique_ptr<port>>;
    using Reactor_Port = std::function<void(Reactor_Command)>;

    FrameWorkDispatcher(IRequestSink& sink);

    void publish_request(Request task);     // 框架层向业务层提交业务请求
    void submit_command(Reactor_Command command);          // 业务层向框架层提交发送/关闭任务
    void register_reactor_port(uint32_t reactor_id, Reactor_Port _cb);
    void unregister_reactor_port(uint32_t reactor_id);

    uint32_t get_reactor_id(const Reactor_Command& command);
    ~FrameWorkDispatcher() = default;

private:
    std::mutex mtx;
    IRequestSink& request_sink;
    Reactor_Ports ports;
};