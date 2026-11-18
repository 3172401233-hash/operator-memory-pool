'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setImmediate } = require('node:timers/promises');
const { MemoryPool } = require('../src/memory-pool');

const PAGE = 1024;

function makePool(pages) {
  return new MemoryPool({ pageSize: PAGE, maxArenaBytes: pages * PAGE });
}

test('concurrent operators share the budget without ever exceeding it', async () => {
  const pool = makePool(16);
  const leases = Array.from({ length: 4 }, (_, i) =>
    pool.lease({ operatorId: `op-${i}`, queryId: 'q1' })
  );

  await Promise.all(
    leases.map(async (lease, idx) => {
      const held = [];
      for (let i = 0; i < 200; i++) {
        const bytes = 1 + ((idx * 7 + i * 13) % 5) * 512; // 1B .. 2.5KB
        const allocation = lease.acquire(bytes);
        if (allocation) {
          held.push(allocation);
          if (held.length > 3) held.shift().release();
        }
        assert.ok(
          pool.reservedBytes <= pool.maxArenaBytes,
          `budget exceeded: ${pool.reservedBytes} > ${pool.maxArenaBytes}`
        );
        if (i % 10 === 0) await setImmediate();
      }
      lease.releaseAll();
    })
  );

  const stats = pool.stats();
  assert.equal(pool.reservedBytes, 0, 'all pages returned after operators finish');
  assert.ok(stats.peakBytes > 0 && stats.peakBytes <= pool.maxArenaBytes);
  assert.equal(stats.allocationCount > 0, true);
  for (let i = 0; i < 4; i++) {
    assert.equal(stats.operators[`op-${i}`].currentBytes, 0);
  }
});

test('short-lived small allocations reuse cached pages and leave no leak', () => {
  const pool = makePool(8);
  const lease = pool.lease({ operatorId: 'scan', queryId: 'q1' });

  for (let round = 0; round < 500; round++) {
    const a = lease.acquire(100);
    const b = lease.acquire(200);
    assert.ok(a && b);
    a.release();
    b.release();
  }

  const stats = pool.stats();
  assert.equal(stats.allocationCount, 1000);
  assert.equal(pool.reservedBytes, 0);
  assert.ok(stats.cachedFreePages <= pool.maxPages, 'page cache stays bounded');
  assert.ok(stats.cachedFreePages > 0, 'freed pages are recycled, not discarded');
  assert.equal(stats.operators.scan.currentBytes, 0);
  lease.close();
});

test('long-lived large blocks cause rejection instead of unbounded growth', () => {
  const pool = makePool(10);
  const hashJoin = pool.lease({ operatorId: 'hash-join', queryId: 'q1' });
  const agg = pool.lease({ operatorId: 'agg', queryId: 'q1' });

  const big = hashJoin.acquire(8 * PAGE); // long-lived hash table
  assert.ok(big);

  const rejected = agg.acquire(4 * PAGE); // needs 4 pages, only 2 free
  assert.equal(rejected, null, 'allocation rejected when budget is exhausted');
  assert.equal(pool.stats().rejectedCount, 1);
  assert.equal(pool.stats().operators.agg.rejections, 1);
  assert.equal(pool.reservedBytes, 8 * PAGE, 'pool did not grow past the rejection');

  big.release(); // hash join finishes its build side
  const ok = agg.acquire(4 * PAGE);
  assert.ok(ok, 'allocation succeeds once budget is free');
  assert.equal(pool.stats().peakBytes, 8 * PAGE);
});

test('cancelling a query releases all of its leases and pages', () => {
  const pool = makePool(10);
  const victim1 = pool.lease({ operatorId: 'join', queryId: 'q-victim' });
  const victim2 = pool.lease({ operatorId: 'sort', queryId: 'q-victim' });
  const survivor = pool.lease({ operatorId: 'scan', queryId: 'q-survivor' });

  victim1.acquire(3 * PAGE);
  victim2.acquire(2 * PAGE);
  const kept = survivor.acquire(4 * PAGE);
  assert.equal(pool.reservedBytes, 9 * PAGE);

  const closed = pool.cancelQuery('q-victim');
  assert.equal(closed, 2);
  assert.equal(pool.reservedBytes, 4 * PAGE, 'only the surviving query keeps memory');
  assert.equal(victim1.open, false);
  assert.equal(victim2.open, false);
  assert.throws(() => victim1.acquire(PAGE), /closed/);

  kept.release();
  survivor.close();
  assert.equal(pool.reservedBytes, 0);
});

test('fragmentation is tracked and pages stay recyclable after out-of-order release', () => {
  const pool = makePool(8);
  const lease = pool.lease({ operatorId: 'projection', queryId: 'q1' });

  const a = lease.acquire(1000); // 1 page, 24B internal waste
  const b = lease.acquire(1500); // 2 pages, 548B waste
  const c = lease.acquire(10); // 1 page, 1014B waste

  assert.equal(lease.pagesHeld, 4);
  assert.equal(lease.fragmentationBytes, 4 * PAGE - (1000 + 1500 + 10));
  assert.equal(pool.fragmentationBytes, lease.fragmentationBytes);
  assert.equal(pool.stats().operators.projection.fragmentationBytes, lease.fragmentationBytes);

  // Release out of order: pages must return to the free cache regardless.
  b.release();
  assert.equal(lease.pagesHeld, 2);
  const d = lease.acquire(PAGE); // reuses one of b's pages
  assert.ok(d);
  assert.equal(lease.pagesHeld, 3);

  a.release();
  c.release();
  d.release();
  assert.equal(pool.reservedBytes, 0);
  assert.equal(pool.fragmentationBytes, 0);
  lease.close();
});

test('budget pressure triggers spill callbacks and the allocation then succeeds', () => {
  const pool = makePool(8);
  const spilled = [];
  const join = pool.lease({
    operatorId: 'hash-join',
    queryId: 'q1',
    onSpill: (lease) => {
      // Spill the oldest buffered partition: release its pages.
      const oldest = lease._allocations.values().next().value;
      if (!oldest) return 0;
      spilled.push(oldest.bytes);
      const pages = oldest.pages.length;
      oldest.release();
      return pages;
    },
  });
  const agg = pool.lease({ operatorId: 'agg', queryId: 'q1' });

  join.acquire(3 * PAGE);
  join.acquire(3 * PAGE); // join holds 6 of 8 pages
  const granted = agg.acquire(4 * PAGE); // needs spill of 2 pages

  assert.ok(granted, 'allocation succeeds after spilling');
  assert.deepEqual(spilled, [3 * PAGE], 'oldest join partition was spilled');
  assert.equal(pool.stats().spillCount, 1);
  assert.equal(pool.stats().operators['hash-join'].spills, 1);
  assert.equal(join.pagesHeld, 3);
  assert.equal(pool.reservedBytes, 7 * PAGE);
});

test('fairness: the largest consumer is asked to spill first', () => {
  const pool = makePool(8);
  const spillOrder = [];
  const makeSpiller = (id) =>
    pool.lease({
      operatorId: id,
      queryId: 'q1',
      onSpill: (lease) => {
        spillOrder.push(id);
        const oldest = lease._allocations.values().next().value;
        if (!oldest) return 0;
        const pages = oldest.pages.length;
        oldest.release();
        return pages;
      },
    });

  const big = makeSpiller('big'); // will hold 4 pages
  const small = makeSpiller('small'); // will hold 2 pages
  big.acquire(4 * PAGE);
  small.acquire(2 * PAGE);

  const newcomer = pool.lease({ operatorId: 'newcomer', queryId: 'q1' });
  const granted = newcomer.acquire(3 * PAGE); // 6 used + 3 needed > 8 pages

  assert.ok(granted);
  assert.equal(spillOrder[0], 'big', 'largest consumer spills first');
  assert.equal(big.pagesHeld, 0, 'big spilled its whole buffer before small was touched');
  assert.equal(small.pagesHeld, 2, 'smaller consumer kept its memory');
});

test('per-lease maxBytes caps a greedy operator', () => {
  const pool = makePool(16);
  const capped = pool.lease({ operatorId: 'capped', queryId: 'q1', maxBytes: 4 * PAGE });
  const other = pool.lease({ operatorId: 'other', queryId: 'q1' });

  assert.ok(capped.acquire(4 * PAGE));
  assert.equal(capped.acquire(PAGE), null, 'lease cap rejects even though pool has room');
  assert.ok(other.acquire(8 * PAGE), 'other operators are unaffected');
  assert.equal(pool.reservedBytes, 12 * PAGE);
});

test('invalid configuration and usage are rejected', () => {
  assert.throws(() => new MemoryPool({ pageSize: 0, maxArenaBytes: 1024 }), RangeError);
  assert.throws(() => new MemoryPool({ pageSize: 1024, maxArenaBytes: 512 }), RangeError);
  const pool = makePool(2);
  assert.throws(() => pool.lease(), TypeError);
  const lease = pool.lease({ operatorId: 'x' });
  assert.throws(() => lease.acquire(0), RangeError);
  assert.throws(() => lease.acquire(-5), RangeError);
  const a = lease.acquire(10);
  a.release();
  a.release(); // double release is a no-op
  assert.equal(pool.reservedBytes, 0);
});
