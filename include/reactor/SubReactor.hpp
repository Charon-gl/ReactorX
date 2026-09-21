#pragma once

#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <queue>
#include "core/Channel.hpp"
#include "core/ReactorBase.hpp"
#include "net/ConnectionToken.hpp"
#include "net/TCPConnection.hpp"

#define INIT_CONNECTION_NUM 1024


class FrameWorkDispatcher;
struct Slot         // 插槽, 长期存在，只换绑connection
{
public:
    std::unique_ptr<TCPConnection> connection;
    uint32_t no;
    counter_64 generations;     // 该计数器用于generation编号，因此只增不减
    int fd;
    bool active;
    
    Slot(uint32_t no, ConnectionInit content);
    void register_connection(uint32_t reactor_id, Channel* channel);
    void unregister_connection();
};

class SubReactor : public ReactorBase
{
public:
    SubReactor(uint32_t reactor_id, FrameWorkDispatcher& dispatcher);

    void set_call_main_reactor(std::function<void(const ReactorFatalEvent&)> _cb);

    ~SubReactor();

    using target_slot = std::pair<bool, std::unordered_map<uint32_t, std::unique_ptr<Slot>>::iterator>;

    SubReactor(SubReactor &&) = delete;
    SubReactor &operator=(SubReactor &&) = delete;
    SubReactor(const SubReactor &) = delete;
    SubReactor &operator=(const SubReactor &) = delete;

private:
    std::unordered_map<uint32_t, std::unique_ptr<Slot>> slots;
    std::queue<uint32_t> free_slots;    // 保存不活跃（没有绑定连接）的插槽序号
    FrameWorkDispatcher& dispatcher;
    uint32_t reactor_id;

    void create_slots();
    void new_connection_event(int fd) override;
    void reactor_fatal_event(int err_no) override;
    void connection_close_event(const ConnectionToken& token, const CloseReason& reason, int err_no) override;
    void response_command(const ConnectionToken& token, std::string data);
    void close_command(const ConnectionToken& token);

    void handle_command(Reactor_Command command);
    target_slot validate(const ConnectionToken& token);        // 校验目标连接是否存在以及是否为同一个连接

    void dispatch_command(Reactor_Command command);

    std::function<void(const ReactorFatalEvent&)> call_main_reactor; // 从reactor给主reactor通信的回调接口

};
