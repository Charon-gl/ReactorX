#include <sys/eventfd.h>
#include <unistd.h>
#include "core/EventLoop.hpp"
#include "utils/Err_Manager.hpp"

EventLoop::EventLoop()
    : is_stop(false), fatal_errno(0)
{
    event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd == -1)
    {
        exit(1);
    }

    epfd = epoll_create(1);
    if (epfd == -1)
    {
        // std::cerr << "Epoll_create failed" << std::endl;
        exit(1);
    }

    evs.resize(MAX_PER_CONNECTION);

    epoll_event ev;
    ev.data.fd = event_fd;
    ev.events = EPOLLIN | EPOLLET;
    
    raw_epoll_ctl(EPOLL_CTL_ADD, &ev, event_fd);
}

void EventLoop::send_wakeup()
{
    int res = eventfd_write(event_fd, 1);
    if (res == -1)
    {
        core_error(errno);
    }
}

void EventLoop::read_wakeup()
{
    eventfd_t val;
    int res = eventfd_read(event_fd, &val);
    if (res == -1)
    {
        core_error(errno);
    }
    run_all_tasks();
}

void EventLoop::loop()
{
    while (!is_stop)
    {
        int nums_fd = epoll_wait(epfd, evs.data(), evs.size(), -1);
        if (nums_fd < 0)
        {
            core_error(errno);
        }
        for (int i = 0; i < nums_fd; ++i)
        {
            int fd = evs[i].data.fd;
            if (fd == event_fd)
                read_wakeup();
            else
                io_event(fd, evs[i].events);
        }
    }
}

bool EventLoop::raw_epoll_ctl(int op, epoll_event *ev, int fd)
{
    int ret = epoll_ctl(epfd, op, fd, ev);
    if (ret == -1)
    {
        core_error(errno);
        // auto error_rank = Err_Manager::err_judge(errno);
        // auto error_event = error_event_package(errno, error_rank, fd);
        // handle_error(error_event);
        // if (error_rank == Err_Rank::CLOSE_CONNECTION || error_rank == Err_Rank::FATAL)
        //     return false;
    }
    return true;
}

void EventLoop::run_all_tasks()
{
    std::queue<std::function<void()>> tmp;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (task_queue.empty())
            return;
        swap(tmp, task_queue);
    }

    while (!tmp.empty())
    {
        tmp.front()();
        tmp.pop();
    }
}

void EventLoop::stop_epoll(int err_no)
{
    is_stop = true;
    fatal_errno = err_no;
}

EventLoop::~EventLoop()
{
    if (event_fd != -1)
        close(event_fd);
    if (epfd != -1)
        close(epfd);
}
