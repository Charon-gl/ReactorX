#include <unordered_set>
#include "reactor/SubReactor.hpp"
#include "net/TCPConnection.hpp"

std::unordered_map<Conn_state, std::unordered_set<Conn_state>> transition_table = {
    {Conn_state::UNCONNECT, {Conn_state::CONNECTED}},
    {Conn_state::CONNECTED, {/*Conn_state::PROCESSING, */Conn_state::CLOSING}},
    //{Conn_state::PROCESSING, {Conn_state::CONNECTED, Conn_state::CLOSING}},
    {Conn_state::CLOSING, {Conn_state::UNCONNECT}}
};

bool is_valid_transition(Conn_state cur_state, Conn_state new_state)        //判断状态转换是否合法
{
    return transition_table[cur_state].count(new_state);
}

TCPConnection::TCPConnection(SubReactor* reactor, std::function<void(Request)> request_callback) 
        : IO_Object{reactor},
            token(ConnectionToken{}),
            send_begin_pos(0),
            state{Conn_state::UNCONNECT},
            close_event_sent(false)
{    
    recv_buf.reserve(MAX_BUF_SIZE);
    send_buf.reserve(MAX_BUF_SIZE);

    set_request_submit(std::move(request_callback));
}

bool TCPConnection::transition(Conn_state new_state)
{
    if(is_valid_transition(state, new_state))
    {
        state = new_state;
        return true;
    }

    return false;
}

void TCPConnection::on_read()
{
    if (state == Conn_state::UNCONNECT || state == Conn_state::CLOSING)
        return;

    int fd = channel->get_fd();
    while(true)
    {
        size_t n = recv_buf.size();
        recv_buf.resize(n + MAX_BUF_SIZE);
        int res = read(fd, recv_buf.data() + n, recv_buf.capacity() - n);
        if(res == 0)
        {   // 正处于processing态，关闭读监听，继续后面的流程；
            // 处于connected状态，可以走关闭流程

            if(request_count.get() == 0 && send_buf.size() == 0)
            // 计数器为0只代表请求已经处理完，但不代表已全部发送完。因此要检查sendbuf是否清0才能关
                on_close();
            else
            {
                transition(Conn_state::CLOSING);
                channel->disable_events(EPOLLIN);
            }
            return;
        }
        else if (res == -1)
        {
            recv_buf.resize(n);
            bool res = on_error(errno);
            if(!res)
                break;
            return;
        }

        // 接收到了，手动更新size
        recv_buf.resize(n + res);
    }

    std::string data;
    data.reserve(MAX_BUF_SIZE);
    swap(data, recv_buf);
    
    submit_request(Request{token, std::move(data)});
    request_count.inc();        // 请求数量+1
    return;
}

void TCPConnection::on_send()
{
    if (state == Conn_state::UNCONNECT)
        return;

    int fd = channel->get_fd();
    while (send_begin_pos < send_buf.size())
    {
        int res = send(
            fd,
            send_buf.data() + send_begin_pos,
            send_buf.size() - send_begin_pos,
            0);
        if(res > 0)
            send_begin_pos += res;
        else if(res < 0)
        {
            on_error(errno);
            return;
        }
    }
    // 执行到这意味着当前写缓冲区的数据已全部发送完毕，可以给写缓冲区清空数据
    send_buf.clear();
    send_begin_pos = 0;
    channel->disable_events(EPOLLOUT);

    if(request_count.get() == 0 && state == Conn_state::CLOSING)     // 响应已经发送完，连接可以关闭了
        on_close();

    return;
}

bool TCPConnection::on_error(int err_no)
{
    auto err_rank = Err_Manager::err_judge(err_no);
    switch (err_rank)
    {
    case Err_Rank::IGNORE:
    case Err_Rank::RETRY:       // 非阻塞io不会EINTR，这里直接忽略即可
        return false;
    default:
        channel->disable_events(EPOLLIN);
        transition(Conn_state::CLOSING);
        on_close(err_no);
    }
    return true;
}

void TCPConnection::on_close(int err_no)
{
    if (close_event_sent)
        return;

    close_event_sent = true;
    transition(Conn_state::CLOSING);
    channel->disable_events(EPOLLIN | EPOLLRDHUP);
    shutdown(channel->get_fd(), SHUT_WR);
    CloseReason res = get_reason(err_no);
    static_cast<SubReactor*>(reactor)->dispatch_event(ConnectionCloseEvent{token, res, err_no});
}

void TCPConnection::activate(Channel* channel, ConnectionToken token_)
{
    token = token_;
    close_event_sent = false;
    recv_buf.reserve(MAX_BUF_SIZE);
    send_buf.reserve(MAX_BUF_SIZE);
    transition(Conn_state::CONNECTED);

    activate_impl(channel);
}

void TCPConnection::deactivate()
{
    token = {};
    recv_buf.clear();
    send_buf.clear();
    send_begin_pos = 0;
    
    deactive_impl();
    transition(Conn_state::UNCONNECT);
}

void TCPConnection::to_send(const char* data, size_t len)
{
    request_count.dec();        // 发送是流式发送，不能确定一次发送了多少个请求，因此当响应数据返回后就将计数器-1,表示1个业务完成
    send_buf.append(data, len);
    channel->enable_events(EPOLLOUT);
}

void TCPConnection::to_close()
{
    if(state != Conn_state::CLOSING)
    {
        channel->disable_events(EPOLLIN);
        transition(Conn_state::CLOSING);
    }
    if(request_count.get() == 0)
        on_close();
}

/*对于多线程/协程的业务处理，需要考虑安全问题，
由于某个连接在进行业务处理的过程中连接可能被突然释放掉，
这时候当业务处理完成调用回调返回时会造成非法访问，在此先提前写下一个思路：

业务处理返回给tcpconnection主要是通过回调或者调用的方式将约为结果存放到tcpconnection的写缓冲区内，
这种方式需要sharedptr和weakptr的使用保证线程安全。但是这就需要去改变现有的底层架构。
我们依然可以通过跨线程投递的方式，业务处理完成后调用一个回调，将业务结果打包成一个任务投回
subreactor的任务队列，然后再由subreactor将其写入到对应的tcpconnection中。

但是这依旧解决不了tcpconnection被另一个连接复用的问题
我们可以设定一个机制：当这个tcpconnection在进行业务处理时，
若连接将被关闭，则让subreactor向业务线程发送该连接已失效的标志位。
业务线程在打包任务时要传入这个标志位以及对应的fd。
当subreactor拿到后检测fd和标志位，只要标志位为false，丢弃该包
只有fd存活且标志位为true时才将该包写入对应的tcpconnection的写缓冲区。

当然这种方式会使得处理一条连接的时间变长。
*/