```
operator-memory-pool
====================
```

执行器内存池：为查询算子提供有界内存预算、页租借（lease）、spill 与查询取消，
杜绝"每个批次单独分配 Buffer"造成的碎片，以及大算子绕过预算导致的连锁 OOM。

## 运行

```bash
node --test          # 运行全部测试
npm run simulate     # 运行模拟负载，输出峰值内存与 spill 次数
```

## 核心概念

池子在启动时拿到固定预算 `maxArenaBytes`，按 `pageSize` 切分成页。
池子**永远不会**超过预算扩张：预算不足时先请求 spill，spill 不出就拒绝分配。

```js
const { MemoryPool } = require('./src/memory-pool');

const pool = new MemoryPool({ pageSize: 64 * 1024, maxArenaBytes: 16 * 64 * 1024 });

const lease = pool.lease({
  operatorId: 'hash-join',
  queryId: 'q1',
  maxBytes: 8 * 64 * 1024,        // 可选：单算子上限
  onSpill: (lease, { pagesNeeded }) => {
    // 把最旧的分区写盘并释放页，返回释放的页数；返回 0 表示无处可 spill
    return spillOldestPartitionToDisk(lease);
  },
});

const alloc = lease.acquire(128 * 1024);  // 返回 Allocation；预算不足时返回 null
alloc.release();                          // 归还页
lease.close();                            // 关闭租借，归还全部页
pool.cancelQuery('q1');                   // 取消查询：关闭其所有 lease
```

## Lease 生命周期

1. **获取**：`pool.lease({ operatorId, queryId, onSpill, maxBytes })`。
   每个算子在初始化时拿一个 lease，`queryId` 用于取消传播。
2. **分配**：`lease.acquire(bytes)` 按页取整分配（`ceil(bytes / pageSize)` 页），
   返回 `Allocation`。预算不足时先触发 spill，仍不足则返回 `null`（拒绝），
   绝不超发。`acquireOrThrow` 在拒绝时抛异常。
3. **释放**：`alloc.release()` 或 `lease.releaseAll()`。页进入池内缓存复用，
   不归还操作系统，避免反复向 V8/OS 要内存造成的碎片。
4. **关闭**：`lease.close()` 释放全部页并注销。`pool.cancelQuery(queryId)`
   会关闭该查询的所有 lease——取消路径与正常结束路径走同一段释放代码，
   保证取消后内存立刻可被兄弟算子使用。

## 公平分配

- **预算硬上限**：所有算子共享 `maxArenaBytes`，任何时刻
  `reservedBytes <= maxArenaBytes`，由测试断言保证。
- **最大者先 spill**：预算紧张时，按持有页数降序请求 spill——
  占用最多的算子（通常是大 join）最先被要求吐内存，而不是饿死小算子。
- **单算子上限**：可选的 `maxBytes` 防止单个贪婪算子独占池子。
- **拒绝而非扩张**：spill 无可 spill 时分配被拒绝并计入
  `rejectedCount`，由调用方决定等待、降级或失败。

## 碎片处理

页大小固定，外部碎片被消除；剩余的是页内碎片（请求量不是页大小整数倍）。
池子按算子统计 `fragmentationBytes = reservedBytes - bytesHeld`，
可通过 `pool.stats()` 观察。释放的页进入缓存直接复用，
短生命周期小分配不会留下不可回收的空洞。

## 可观测性

`pool.stats()` 返回：

- `peakBytes` — 峰值内存（全程不超过 `maxArenaBytes`）
- `spillCount` / `rejectedCount` / `allocationCount`
- `fragmentationBytes` — 当前页内碎片总量
- `operators[id]` — 每个算子的 `currentBytes`、`peakBytes`、`spills`、`rejections`

## 测试覆盖

`node --test` 覆盖：并发算子共享预算、短生命周期小分配的页复用、
长生命周期大块导致的拒绝（而非无限扩张）、查询取消释放、
乱序释放后的碎片统计与页回收、spill 触发与"最大者先 spill"的公平性、
单算子上限、非法参数。
