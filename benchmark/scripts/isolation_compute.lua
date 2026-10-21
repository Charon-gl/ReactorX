-- ═══════════════════════════════════════════════════════════════════════════
-- 混合负载 A: 短任务 + compute（CPU 密集）
--
-- 路由分配（per-thread 确定性分流）：
--   /fast             90%  → sleep 1ms，代表正常短时任务
--   /compute?n=N      10%  → CPU 密集循环，代表计算任务阻塞
--
-- compute 迭代次数通过环境变量 REACTORX_COMPUTE_ITERS 控制，
-- 默认 3000000（约 10ms CPU）。
--
-- 用法：
--   REACTORX_COMPUTE_ITERS=3000000 wrk -t4 -c2000 -d120s -R2000 \
--       --latency --timeout 5s \
--       -s benchmark/scripts/isolation_compute.lua \
--       http://127.0.0.1:6666
-- ═══════════════════════════════════════════════════════════════════════════

local compute_n = os.getenv("REACTORX_COMPUTE_ITERS") or "3000000"
local counter = 0

function request()
    counter = counter + 1
    if counter % 100 < 90 then
        return wrk.format("GET", "/fast")
    else
        return wrk.format("GET", "/compute?n=" .. compute_n)
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