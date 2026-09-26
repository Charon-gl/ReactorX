#pragma once

#include <cstdint>
#include <functional>

struct ConnectionToken      // 连接的标识号，每个连接唯一
{
    uint32_t reactor_id;
    uint32_t slot;          // 插槽位，SubReactor会维护一个容器用于存储当前的所有连接，此处存储的是该连接所在的槽位
    uint64_t generation;    // 插槽的版本，即该连接是这个插槽管理的第几个

    ConnectionToken() : slot(-1), generation(-1) {}
    ConnectionToken(uint32_t reactor_id, uint32_t slot, uint64_t generation) : reactor_id(reactor_id), slot(slot), generation(generation) {}
    ConnectionToken(const ConnectionToken& other) { *this = other; }

    ConnectionToken& operator=(const ConnectionToken& other)
    {
        if(*this == other)
            return *this;
        reactor_id = other.reactor_id;
        slot = other.slot;
        generation = other.generation;
        return *this;
    }
    bool operator==(const ConnectionToken& other) const
    {
        return reactor_id == other.reactor_id && slot == other.slot && generation == other.generation;
    }
    bool operator!=(const ConnectionToken& other) const { return !(*this == other); }
};

// class ConnectionHandler    // 一个句柄，传给上层业务/协议，每个句柄绑定了一个连接，并提供了与框架层通信的回调
// {
// public:
//     ConnectionHandler(const ConnectionToken&);
//     ConnectionToken token() const noexcept;

//     std::function<void()> send;
//     std::function<void()> close;
//     bool alive() const;

// private:
//     ConnectionToken token;
// };