#include <iostream>
#include "reactor/MainReactor.hpp"
#include "net/Acceptor.hpp"

Acceptor::Acceptor(MainReactor* mainreactor) : IO_Object(mainreactor), port(0)
{
    memset(&addr, 0, sizeof(addr));
}

int Acceptor::init_listen_fd(uint16_t port_)
{

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd == -1)
    {
        std::cerr << "Socket failed" << std::endl;
        return -1;
    }

    port = port_;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int ret = bind(lfd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    if (ret == -1)
    {
        std::cerr << "Bind failed" << std::endl;
        close(lfd);
        return -1;
    }

    ret = listen(lfd, SOMAXCONN);
    if (ret == -1)
    {
        std::cerr << "Listen failed" << std::endl;
        close(lfd);
        return -1;
    }

    int flat = fcntl(lfd, F_GETFL);
    fcntl(lfd, F_SETFL, flat | O_NONBLOCK);

    return lfd;
}

void Acceptor::activate(Channel* channel)
{
    channel->set_read_cb([this]{
        on_read();
    });
    channel->set_send_cb([this]{
        on_send();
    });
    channel->set_error_cb([this](int err_no){
        on_error(err_no);
    });

    activate_impl(channel);
}

void Acceptor::deactivate()
{
    memset(&addr, 0, sizeof(addr));
    deactivate_impl();
}

void Acceptor::on_read()
{
    sockaddr_in caddr;
    memset(&caddr, 0, sizeof(caddr));
    
    while (true)
    {
        socklen_t len = sizeof(caddr);
        int cfd = accept(channel->get_fd(), reinterpret_cast<sockaddr*>(&caddr), &len);
        if (cfd == -1)
        {
            bool res = on_error(errno);
            if(res)    // EINTR
                continue;       // 冷处理则会把-1当成新连接传给subreactor
                
            break;
        }
        // 投递新连接
        static_cast<MainReactor*>(reactor)->round_dispatch(cfd); // 可以考虑扩展成批量投递
    }
}

bool Acceptor::on_error(int err_no)
{
    auto err_rank = Err_Manager::err_judge(err_no);
    switch (err_rank)
    {
    case Err_Rank::RETRY:
        return true;
    case Err_Rank::IGNORE:
        break;
    default:
     // 如果是延迟关闭，那就要先注销读监听，避免在删除前来读事件；其实这里更推荐直接删掉
        on_close(err_no);
        break;
    }
    return false;
}

void Acceptor::on_close(int err_no)
{
    static_cast<MainReactor*>(reactor)->remove_listener(channel->get_fd(), get_reason(err_no), err_no);
}

Acceptor::~Acceptor()
{
    deactivate();
}