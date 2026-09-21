#include "reactor/MainReactor.hpp"
#include <cstdlib>
#include <arpa/inet.h>
#include <sys/eventfd.h>
#include "utils/Err_Manager.hpp"


/*-------------MainReactor--------------*/
MainReactor::MainReactor(uint16_t port, int core_num, FrameWorkDispatcher& dispatcher)
{
    sub_reactor_threads.reserve(core_num);
    sub_reactors.reserve(core_num);
    active_listeners.reserve(MAX_PER_CONNECTION);

    // subreactor
    for (int i = 0; i < core_num; ++i)
    {
        add_sub_reactor(std::ref(dispatcher));
    }
    
    // 开一个listener
    auto res = add_listener(port);
    if(!res)
        exit(1);    // 初始化失败，直接终止程序
    
    round_ptr = sub_reactors.begin();
}

void MainReactor::round_dispatch(int fd)
{
    round_ptr->second->dispatch_event(NewConnectionEvent{fd});
    update_round_ptr();
}

void MainReactor::update_round_ptr()
{
    ++round_ptr;
    if (round_ptr == sub_reactors.end())    // 说明迭代器失效/轮询到最后一个了
        round_ptr = sub_reactors.begin();
}

void MainReactor::add_sub_reactor(FrameWorkDispatcher& dispatcher)
{
    uint32_t num = reactor_counter.get();
    auto subreactor = std::make_unique<SubReactor>(num, dispatcher);
    subreactor->set_call_main_reactor([this](ReactorFatalEvent event) { 
        dispatch_event(event); 
    });
    std::thread t([subreactor = subreactor.get()] { 
        subreactor->run(); 
    });
    
    
    sub_reactor_threads.emplace(num, std::make_unique<std::thread>(std::move(t)));
    sub_reactors.emplace(num, std::move(subreactor));
    reactor_counter.inc();
    round_ptr = sub_reactors.begin();
}

bool MainReactor::add_listener(uint16_t port)
{
    // 初始化,然后加入active_listeners
    auto acceptor = std::make_unique<Acceptor>(this);
    auto channel = acceptor->active(port);
    if(channel == nullptr)
        return false;

    active_listeners.emplace(channel->get_fd(), std::move(acceptor));
    register_channel(std::move(channel));

    return true;
}

void MainReactor::remove_listener(int fd, CloseReason reason, int err_no)
{
    auto it = active_listeners.find(fd);
    if(it != active_listeners.end() && it->second)
    {
        unregister_channel(fd);
        // active_listeners[fd]->deactive();
        active_listeners.erase(it);
    }
}

void MainReactor::reactor_fatal_event(int err_no)
{
    // 先停止所有listener
    std::vector<int> listeners;
    listeners.reserve(active_listeners.size());

    for (const auto &i : active_listeners)
        listeners.push_back(i.first);

    for(int fd : listeners)
        remove_listener(fd, CloseReason::EPOLL_FATAL, 0);

    // 再停止从reactor
    for (auto &i : sub_reactors)
    {
        i.second->dispatch_event(ReactorFatalEvent{i.first, err_no});
    }
    for(auto& i : sub_reactor_threads)
        i.second->join();

    stop_epoll(err_no);
}

void MainReactor::reactor_close_event(uint32_t reactor_id, int err_no)
{
    auto it = sub_reactors.find(reactor_id);
    if(it == sub_reactors.end())
        return;
        
    // 记录日志

    sub_reactor_threads[reactor_id]->join();
    sub_reactors.erase(it);
    sub_reactor_threads.erase(reactor_id);
    
    round_ptr = sub_reactors.begin();

    if(sub_reactor_threads.empty())
    {
        // 已经没有subreactor了，因此也需要全站关闭，就地删除，不再延后
        std::vector<int> listeners;
        listeners.reserve(active_listeners.size());

        for (const auto &i : active_listeners)
            listeners.push_back(i.first);

        for(int fd : listeners)
            remove_listener(fd, CloseReason::EPOLL_FATAL, 0);
    }
}

MainReactor::~MainReactor()
{
    sub_reactors.clear();
    sub_reactor_threads.clear();
}

