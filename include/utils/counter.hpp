#pragma once
#include <iostream>
#include <cstdint>
#include <atomic>



template <typename T>
class Counter
{
private:
    T count_;

public:
    Counter() : count_(0) {}

    bool inc() 
    { 
        if(count_ == UINT64_MAX)
            return false;
        ++count_; 
        return true;
    }

    bool dec() 
    { 
        if(count_ <= 0)
            return false;
        --count_; 
        return true;
    }

    void reset() { count_ = 0; }

    u_int64_t get() const noexcept { return count_; }
};

class atomic_counter
{
private:
    std::atomic<uint64_t> count_;

public:
    atomic_counter() : count_(0) {}

    bool inc() 
    {
        auto val = count_.load(std::memory_order_relaxed);
        if(val == UINT64_MAX)
            return false;

        while(!count_.compare_exchange_weak(val, val + 1, std::memory_order_relaxed));
    }
};


using counter_16 = Counter<uint16_t>;
using counter = Counter<uint32_t>;
using counter_64 = Counter<uint64_t>;