#pragma once

#include <thread>
#include <functional>
#include <atomic>
#include "utils/counter.hpp"
#include "core/ReactorEvent.hpp"

struct port_
{
    std::function<void(Operation)> submit_cb;
    virtual void submit(Operation op) = 0;
    virtual void unregister() = 0;

    explicit port_(std::function<void(Operation)> _cb) : submit_cb(std::move(_cb)) {}
    virtual ~port_() = default;
};

struct async_port : port_
{
    std::atomic<bool> alive;
    atomic_counter using_count;

    async_port(std::function<void(Operation)> _cb) : port_{std::move(_cb)}, alive(true) {}
    void submit(Operation op) override 
    {
        using_count.inc(1, std::memory_order_seq_cst);      // 必须先占位
        if(alive.load(std::memory_order_acquire))
        {
            submit_cb(std::move(op));
        }
        using_count.dec(1, std::memory_order_release, std::memory_order_relaxed);
    }

    void unregister() override 
    { 
        alive.store(false, std::memory_order_seq_cst);      // 写后读场景，必须让所有线程能够看见，且写不能发生在读之后
        while(using_count.get(std::memory_order_acquire) != 0)   // 等待直到所有访问该回调的对象已退出
            std::this_thread::yield();        // 暂时让出cpu
        
        submit_cb = nullptr; 
    }
};

struct sync_port : port_
{
    sync_port(std::function<void(Operation)> _cb) : port_{std::move(_cb)} {}
    void submit(Operation op) override 
    {
        if(submit_cb)
            submit_cb(std::move(op));
    }

    void unregister() override { submit_cb = nullptr; }
};
