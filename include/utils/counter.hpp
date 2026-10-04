#pragma once
#include <iostream>
#include <cstdint>
#include <atomic>
#include <limits>
#include <type_traits>

// 该头文件实现的是通用的 无符号整型统计计数器，它们允许发生上溢，回绕到0，但不允许发生下溢


template <typename T>
class Counter
{
private:
    T count_;

    static_assert(std::is_integral_v<T>);
    static_assert(!std::is_same_v<T, bool>);
    static_assert(std::is_unsigned_v<T>);

    //static constexpr T counter_min() noexcept { return std::numeric_limits<T>::min(); }
    //static constexpr T counter_max() noexcept { return std::numeric_limits<T>::max(); }

public:
    Counter() : count_(0) {}

    void inc(T val = 1)  { count_ += val;  }

    void dec(T val = 1) 
    {
        if(count_ <=  val)
            count_ = 0;
        else  
            count_ -= val;  
    }

    void reset() { count_ = 0; }

    T get() const noexcept { return count_; }
};


// 原子计数器，递增递减时可以根据使用场景自行决定内存序
template <typename T>
class atomic_Counter
{
private:
    std::atomic<T> count_;

    static_assert(std::is_integral_v<T>);
    static_assert(!std::is_same_v<T, bool>);
    static_assert(std::is_unsigned_v<T>);

    //static constexpr T counter_min() noexcept { return std::numeric_limits<T>::min(); }
    //static constexpr T counter_max() noexcept { return std::numeric_limits<T>::max(); }

public:
    atomic_Counter() : count_(0) {}

    void inc(T val = 1, std::memory_order order = std::memory_order_seq_cst)  { count_.fetch_add(val, order); }

    void dec(T val = 1, std::memory_order suc = std::memory_order_seq_cst, std::memory_order fail = std::memory_order_seq_cst)
    {// 递减操作通过CAS实现，因此这里的load操作只需要relaxed就够了，同时保证不会下溢
        T expected = count_.load(std::memory_order_relaxed);
        while(true)
        {
            T desired = (expected < val) ? 0 : (expected - val);
            if(count_.compare_exchange_weak(expected, desired, std::memory_order_release, std::memory_order_relaxed))
                return;
        }
    }

    T get(std::memory_order order = std::memory_order_seq_cst)const noexcept { return count_.load(order); }
};


using counter16 = Counter<uint16_t>;
using counter = Counter<uint32_t>;
using counter64 = Counter<uint64_t>;

using atomic_counter = atomic_Counter<uint32_t>;
