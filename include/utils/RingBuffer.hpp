#pragma once
#include <iostream>
#include <vector>

template <typename T>
class RingBuffer
{
public:
    std::vector<T> data_;
    size_t head, size, tail;

    RingBuffer() : head(0), tail(0), size(0) {};
    RingBuffer(size_t n)
    {
        RingBuffer();
        data_.reserve(n);
        size = n;
    }

    void push(T data)
    {
        data_.insert(data_.begin() + tail, data);

    }

    T pop_front()
    {

    }

    T pop_back()
    {

    }

    RingBuffer(const RingBuffer &) = delete;
    RingBuffer &operator=(const RingBuffer &) = delete;
    RingBuffer(RingBuffer &&) = delete;
    RingBuffer &operator=(RingBuffer &&) = delete;
};