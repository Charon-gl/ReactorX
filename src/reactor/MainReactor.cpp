#include "reactor/MainReactor.hpp"
#include <cstring>
#include <arpa/inet.h>
#include <sys/eventfd.h>
#include "utils/Err_Manager.hpp"


/*-------------MainReactor--------------*/
MainReactor::MainReactor(uint16_t port, int core_num) : round_ptr(0)
{
    sub_reactor_threads.reserve(core_num);
    sub_reactors.reserve(core_num);
    active_listeners.reserve(MAX_PER_CONNECTION);

    // 开一个listener
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    add_listener(addr);

    // subreactor
    for (int i = 0; i < core_num; ++i)
    {
        add_sub_reactor();
    }
}

MainReactor& MainReactor::instance(uint16_t port, int core_num)
{
    static MainReactor acceptor(port, core_num);
    return acceptor;
}

void MainReactor::round_dispatch(int fd)
{
    round_ptr->second->dispatch_event(NewConnectionEvent{fd});
    update_round_ptr();
}

void MainReactor::update_round_ptr() // 是有缺陷的，当增减subreactor时某个/某些可能会被跳过
{
    auto it = sub_reactors.find(round_ptr->first);
    if (it == sub_reactors.end() || std::next(it) == sub_reactors.end())    // 说明迭代器失效/轮询到最后一个了
        round_ptr = sub_reactors.begin();
    round_ptr = ++it;
}

void MainReactor::add_sub_reactor()
{
    auto subreactor = std::make_unique<SubReactor>();
    subreactor->set_call_main_reactor([this](ReactorFatalEvent event) { 
        dispatch_event(event); 
    });
    std::thread t([subreactor = subreactor.get()] { 
        subreactor->run(); 
    });

    std::thread::id tid = t.get_id();

    sub_reactor_threads.emplace(tid, std::make_unique<std::thread>(std::move(t)));
    sub_reactors.emplace(tid, std::move(subreactor));
    update_round_ptr();
}

void MainReactor::add_listener(const sockaddr_in& addr)
{
    // 初始化,然后加入active_listeners
    auto acceptor = std::make_unique<Acceptor>(this);
    acceptor->active(addr);
    active_listeners.emplace(acceptor->get_fd(), std::move(acceptor));
}

void MainReactor::remove_listener(int fd, CloseReason reason, int err_no)
{
    if(active_listeners[fd])
    {// 延迟关闭,其实这里更推荐直接删掉
        dispatch_event(ConnectionCloseEvent{fd, reason, err_no});
        // active_listeners[fd]->deactive();
        active_listeners.erase(fd);
    }
}

void MainReactor::reactor_fatal_event(int err_no)
{
    // 先停止所有listener
    for (auto &i : active_listeners)
        remove_listener(i.first, CloseReason::EPOLL_FATAL, 0);

    // 再停止从reactor
    for (auto &i : sub_reactors)
    {
        i.second->dispatch_event(ReactorFatalEvent{std::this_thread::get_id(), err_no});
    }
    for(auto& i : sub_reactor_threads)
        i.second->join();

    stop_epoll(err_no);
}

void MainReactor::reactor_close_event(std::thread::id thread_id, int err_no)
{
    auto it = sub_reactors.find(thread_id);
    if(it != sub_reactors.end())
    {
        // 记录日志

        sub_reactor_threads[thread_id]->join();
        sub_reactors.erase(it);
        sub_reactor_threads.erase(thread_id);
        update_round_ptr();
    }

    if(sub_reactor_threads.empty())
    {
        // 已经没有subreactor了，因此也需要全站关闭，就地删除，不再延后
        for(auto &i : active_listeners)
        {
            int fd = i.first;
            active_listeners.erase(fd);
            unregister_channel(fd);
        }
    }
}

MainReactor::~MainReactor()
{
    sub_reactors.clear();
    sub_reactor_threads.clear();
}

