#include "net/FrameWorkDispatcher.hpp"

FrameWorkDispatcher::FrameWorkDispatcher(IRequestSink& sink)
    : request_sink(sink)  {}

void FrameWorkDispatcher::publish_request(Request task) { request_sink.submit(task); }

void FrameWorkDispatcher::submit_command(Reactor_Command command)
{
    uint32_t reactor_id = get_reactor_id(command);
    auto it = ports.find(reactor_id);
    if(it != ports.end())
    {
        std::lock_guard<std::mutex> lock(it->second->mtx);
        if(it->second->active)
            it->second->submit_command(command);     // 只是把command放进任务队列，时间很短，直接在锁内进行避免uaf
    }
}

void FrameWorkDispatcher::register_reactor_port(uint32_t reactor_id, Reactor_Port _cb)
{// 目前该函数不支持运行时注册投递端口
    std::lock_guard<std::mutex> lock(mtx);
    ports.try_emplace(reactor_id, std::make_unique<port>(std::move(_cb)));
}

void FrameWorkDispatcher::unregister_reactor_port(uint32_t reactor_id)
{
    auto it = ports.find(reactor_id);
    if(it != ports.end())
    {
        std::lock_guard<std::mutex> lock(it->second->mtx);
        it->second->active = false;
        it->second->submit_command = nullptr;
    }
}

uint32_t FrameWorkDispatcher::get_reactor_id(const Reactor_Command& command)
{
    return std::visit([](const auto& payload){
        return payload.token.reactor_id;
    }, command.payload);
}