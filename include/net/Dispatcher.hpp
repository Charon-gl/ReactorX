#pragma once 
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include "port.hpp"



enum class mode
{
    sync,
    async
};


class Dispatcher
{
public:
    using Reactor_Ports = std::unordered_map<uint32_t, std::unique_ptr<port_>>;
    using Reactor_Port = std::function<void(Operation)>;
    using Business_CallBack = std::function<void(ByteStream)>;
    using Close_CallBack = std::function<void(ConnectionToken)>;

    explicit Dispatcher() : mode_(mode::sync) {}    // 默认同步模型

    // 框架层向业务层提交业务请求
    void publish_stream(ByteStream stream) 
    { 
        if(business_cb)
            business_cb(stream); 
    }

    void publish_close(ConnectionToken token)
    {
        if(close_cb)
            close_cb(token);
    }

    void submit_operation(Operation operation)           // 业务层向框架层提交发送/关闭任务
    {
        uint32_t reactor_id = get_reactor_id(operation);
        auto it = ports.find(reactor_id);
        if(it != ports.end())
            it->second->submit(std::move(operation));
    }

    void register_reactor_port(uint32_t reactor_id, Reactor_Port _cb)
    {// 目前该函数不支持运行时注册投递端口
        if(mode_ == mode::sync)
        {
            std::lock_guard<std::mutex> lock(mtx);
            ports.try_emplace(reactor_id, std::make_unique<sync_port>(std::move(_cb)));
        }
        else if(mode_ == mode::async)
        {
            std::lock_guard<std::mutex> lock(mtx);
            ports.try_emplace(reactor_id, std::make_unique<async_port>(std::move(_cb))); 
        }

    }

    void unregister_reactor_port(uint32_t reactor_id)
    {
        auto it = ports.find(reactor_id);
        if(it != ports.end())
            it->second->unregister();
    }

    uint32_t get_reactor_id(const Operation& operation)
    {
        return 
        std::visit(
            [](const auto& payload)
            { return payload.token.reactor_id; }, 
            operation.payload
        );
    }

    void init_mode(mode business_mode) { mode_ = business_mode; }
    void set_business_callback(Business_CallBack cb) { business_cb = std::move(cb); }
    void set_close_callback(Close_CallBack cb) { close_cb = std::move(cb); }

    ~Dispatcher() = default;

private:
    std::mutex mtx;
    Business_CallBack business_cb;
    Close_CallBack close_cb;
    Reactor_Ports ports;
    mode mode_;
};
