#include "reactor/MainReactor.hpp"
#include <cstring>
#include <arpa/inet.h>
#include <sys/eventfd.h>
#include "utils/Err_Manager.hpp"


/*-------------MainReactor--------------*/
MainReactor::MainReactor(uint16_t port, int core_num, std::function<void(TCPConnection*)> _cb) 
{
    sub_reactor_threads.reserve(core_num);
    sub_reactors.reserve(core_num);
    active_listeners.reserve(MAX_PER_CONNECTION);

    // subreactor
    for (int i = 0; i < core_num; ++i)
    {
        add_sub_reactor(std::move(_cb));
    }
    
    // 开一个listener
    auto res = add_listener(port);
    if(!res)
        exit(1);    // 初始化失败，直接终止程序
    
    round_ptr = sub_reactors.begin();
}

MainReactor& MainReactor::instance(uint16_t port, int core_num, std::function<void(TCPConnection*)> _cb)
{
    static MainReactor acceptor(port, core_num, std::move(_cb));
    return acceptor;
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

void MainReactor::add_sub_reactor(std::function<void(TCPConnection*)> _cb)
{
    auto subreactor = std::make_unique<SubReactor>(std::move(_cb));
    subreactor->set_call_main_reactor([this](ReactorFatalEvent event) { 
        dispatch_event(event); 
    });
    std::thread t([subreactor = subreactor.get()] { 
        subreactor->run(); 
    });

    std::thread::id tid = t.get_id();

    sub_reactor_threads.emplace(tid, std::make_unique<std::thread>(std::move(t)));
    sub_reactors.emplace(tid, std::move(subreactor));
    round_ptr = sub_reactors.begin();
}

bool MainReactor::add_listener(uint16_t port)
{
    // 初始化,然后加入active_listeners
    auto acceptor = std::make_unique<Acceptor>(this);
    auto res = acceptor->active(port);
    if(!res)
        return false;
    active_listeners.emplace(acceptor->get_fd(), std::move(acceptor));

    return true;
}

void MainReactor::remove_listener(int fd, CloseReason reason, int err_no)
{
    auto it = active_listeners.find(fd);
    if(it != active_listeners.end() && it->second)
    {// 延迟关闭,其实这里更推荐直接删掉
        dispatch_event(ConnectionCloseEvent{fd, reason, err_no});
        // active_listeners[fd]->deactive();
        active_listeners.erase(it);
    }
}

void MainReactor::reactor_fatal_event(int err_no)
{
    // 先停止所有listener
    std::vector<int> listeners;
    listeners.reserve(active_listeners.size());
    for (auto &i : active_listeners)
        listeners.push_back(i.first);

    for(int fd : listeners)
        remove_listener(fd, CloseReason::EPOLL_FATAL, 0);

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
        
        round_ptr = sub_reactors.begin();
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

