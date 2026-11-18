'use strict';

/**
 * Simulates one query with three operators competing for a bounded arena:
 *  - scan:       short-lived small batches (acquire/release per batch)
 *  - hash-join:  long-lived large hash table, spillable partition by partition
 *  - agg:        medium, non-spillable
 *
 * Prints peak memory and spill count at the end.
 */

const { MemoryPool } = require('../src/memory-pool');

const PAGE = 64 * 1024; // 64 KiB pages
const pool = new MemoryPool({ pageSize: PAGE, maxArenaBytes: 16 * PAGE }); // 1 MiB arena

const joinSpilledPartitions = [];
const join = pool.lease({
  operatorId: 'hash-join',
  queryId: 'q1',
  onSpill: (lease) => {
    const oldest = lease._allocations.values().next().value;
    if (!oldest) return 0;
    joinSpilledPartitions.push(oldest.bytes);
    const pages = oldest.pages.length;
    oldest.release();
    return pages;
  },
});
const scan = pool.lease({ operatorId: 'scan', queryId: 'q1' });
const agg = pool.lease({ operatorId: 'agg', queryId: 'q1' });

// Join builds its hash table partition by partition (long-lived large blocks).
const joinPartitions = [];
for (let i = 0; i < 6; i++) {
  const part = join.acquire(2 * PAGE);
  if (part) joinPartitions.push(part);
}

// Aggregation reserves a medium buffer for the whole query.
agg.acquire(2 * PAGE);

// Scan chews through many short-lived batches.
let scanBatches = 0;
let scanRejected = 0;
for (let batch = 0; batch < 200; batch++) {
  const a = scan.acquire(PAGE / 2);
  if (a) {
    scanBatches += 1;
    a.release();
  } else {
    scanRejected += 1;
  }
}

// Join probes: needs one more big burst, forcing a spill of its own partitions.
const probe = join.acquire(4 * PAGE);
if (probe) probe.release();

// Query finishes: everything is released (or cancelled).
pool.cancelQuery('q1');

const stats = pool.stats();
const fmt = (bytes) => `${(bytes / 1024).toFixed(1)} KiB`;
console.log('=== query finished ===');
console.log(`peak memory:      ${fmt(stats.peakBytes)} / ${fmt(stats.maxArenaBytes)}`);
console.log(`spill count:      ${stats.spillCount}`);
console.log(`rejected allocs:  ${stats.rejectedCount} (scan batches rejected: ${scanRejected})`);
console.log(`allocations:      ${stats.allocationCount} (scan batches served: ${scanBatches})`);
console.log(`residual memory:  ${stats.reservedBytes} bytes`);
console.log('per-operator:');
for (const [id, s] of Object.entries(stats.operators)) {
  console.log(
    `  ${id.padEnd(10)} peak=${fmt(s.peakBytes)}  spills=${s.spills}  rejections=${s.rejections}  current=${s.currentBytes}B`
  );
}
console.log(`join partitions spilled to disk: ${joinSpilledPartitions.length}`);
