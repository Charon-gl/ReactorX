-- ═══════════════════════════════════════════════════════════════════════════
-- 混合负载 B: 短任务 + delay（IO 等待）
--
-- 路由分配（per-thread 确定性分流）：
--   /fast             90%  → sleep 1ms，代表正常短时任务
--   /slow?ms=N        10%  → sleep N ms，代表慢 IO 阻塞任务
--
-- delay 时长通过环境变量 REACTORX_DELAY_MS 控制，默认 300ms。
--
-- 用法：
--   REACTORX_DELAY_MS=300 wrk -t4 -c2000 -d120s -R2000 \
--       --latency --timeout 5s \
--       -s benchmark/scripts/isolation_delay.lua \
--       http://127.0.0.1:6666
-- ═══════════════════════════════════════════════════════════════════════════

local delay_ms = os.getenv("REACTORX_DELAY_MS") or "300"
local counter = 0

function request()
    counter = counter + 1
    if counter % 100 < 90 then
        return wrk.format("GET", "/fast")
    else
        return wrk.format("GET", "/slow?ms=" .. delay_ms)
    end
end

function done(summary, latency, requests)
    io.write("------------------------------\n")
    io.write(string.format("Requests:    %d\n", summary.requests))
    io.write(string.format("Duration:    %.0f us\n", summary.duration))
    io.write(string.format("Req/Sec:     %.2f\n", summary.requests / (summary.duration / 1000000)))
    io.write(string.format("Errors:      connect=%d read=%d write=%d timeout=%d status=%d\n",
        summary.errors.connect, summary.errors.read,
        summary.errors.write, summary.errors.timeout, summary.errors.status))
    io.write("\nLatency percentiles (us):\n")
    io.write(string.format("  p50:   %.2f\n", latency:percentile(50)))
    io.write(string.format("  p90:   %.2f\n", latency:percentile(90)))
    io.write(string.format("  p99:   %.2f\n", latency:percentile(99)))
    io.write(string.format("  p99.9: %.2f\n", latency:percentile(99.9)))
end