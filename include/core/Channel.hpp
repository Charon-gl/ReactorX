#pragma once

#include <iostream>
#include <functional>
#include <atomic>
#include <stdint.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <memory>

enum class event_state
{
    DOING,
    DONE,
    WRONG
};
class Channel
{
private:
    int fd;
    uint32_t events;
    int _errno;
    std::atomic<bool> valid;        //标志位，true表示fd有效
    std::function<void(Channel *)> update_events;

    std::function<void()> trigger_read;
    std::function<void()> trigger_send;
    std::function<void(int)> trigger_error;

public:
    Channel(int);

    void handle_events(uint32_t); // 接收来自epoll的事件，并传给业务层

    void enable_events(uint32_t tar_events);
    void disbale_events(uint32_t tar_events);
    void clear_events();
    
    int get_fd() const;
    u_int32_t get_events() const;

    void set_read_cb(std::function<void()> _cb);
    void set_send_cb(std::function<void()> _cb);
    void set_error_cb(std::function<void(int)> _cb);

    Channel(Channel &&) = delete;
    Channel &operator=(Channel &&) = delete;
    Channel(const Channel &) = delete;
    Channel &operator=(const Channel &) = delete;
    
    ~Channel();
};