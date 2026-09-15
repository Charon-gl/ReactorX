#pragma once
#include <iostream>
#include <memory>
#include <queue>
#include <functional>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <sys/epoll.h>
#include <fcntl.h>
#include "core/ReactorEvent.hpp"

#define MAX_PER_CONNECTION 32768

class EventLoop // 该类主要实现Reactor的通信逻辑，如何传递信息/任务以及如何执行
{
private:
    int epfd;
    int event_fd;
    std::vector<epoll_event> evs;
    std::queue<std::function<void()>> task_queue;
    std::mutex mtx;
    bool is_stop;

protected:
    int fatal_errno;
    EventLoop(); // 初始化eventfd

    void loop();
    void send_wakeup();
    void read_wakeup();

    template <typename Event>
    void run_in_loop(Event &&event) // 跨线程传递任务
    {
        {
            std::lock_guard<std::mutex> lock(mtx);
            task_queue.emplace(std::forward<Event>(event));
        }
        send_wakeup();
    }

    bool raw_epoll_ctl(int op, epoll_event *ev, int fd);
    void run_all_tasks();                       // 执行任务
    virtual void core_error(int err_no) {};     // eventfd出错的处理入口
    virtual void io_event(int fd, uint32_t events) {};     // epoll事件分发
    void stop_epoll(int err_no = 0);

    int get_epfd() const noexcept;
    int get_event_fd() const noexcept;

    ~EventLoop();
};