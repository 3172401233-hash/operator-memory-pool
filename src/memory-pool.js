'use strict';

/**
 * Execution memory pool.
 *
 * The pool owns a fixed budget (maxArenaBytes) divided into fixed-size pages.
 * Operators acquire memory only through a Lease. When the budget is exhausted
 * the pool asks the largest consumers to spill; if nothing can be spilled the
 * allocation is rejected. The pool never grows beyond maxArenaBytes.
 */

class Allocation {
  constructor(lease, bytes, pages) {
    this.lease = lease;
    this.bytes = bytes;
    this.pages = pages;
    this.released = false;
  }

  release() {
    this.lease.release(this);
  }
}

class Lease {
  constructor(pool, { operatorId, queryId = null, onSpill = null, maxBytes = null }) {
    this.pool = pool;
    this.operatorId = operatorId;
    this.queryId = queryId;
    this.onSpill = onSpill;
    this.maxBytes = maxBytes;
    this.open = true;
    this._allocations = new Set();
    this._bytesHeld = 0;
    this._pagesHeld = 0;
  }

  get bytesHeld() {
    return this._bytesHeld;
  }

  get pagesHeld() {
    return this._pagesHeld;
  }

  get reservedBytes() {
    return this._pagesHeld * this.pool.pageSize;
  }

  get fragmentationBytes() {
    return this.reservedBytes - this._bytesHeld;
  }

  /**
   * Acquire `bytes` of memory. Returns an Allocation, or null when the
   * request is rejected because the budget (or this lease's cap) cannot
   * satisfy it even after spilling.
   */
  acquire(bytes) {
    if (!this.open) throw new Error(`lease for operator "${this.operatorId}" is closed`);
    if (!Number.isFinite(bytes) || bytes <= 0) {
      throw new RangeError(`bytes must be a positive number, got ${bytes}`);
    }
    const pagesNeeded = Math.ceil(bytes / this.pool.pageSize);
    if (this.maxBytes != null && this.reservedBytes + pagesNeeded * this.pool.pageSize > this.maxBytes) {
      this.pool._noteRejected(this);
      return null;
    }
    let pages = this.pool._tryAcquirePages(pagesNeeded);
    if (pages === null) {
      this.pool._requestSpill(pagesNeeded);
      pages = this.pool._tryAcquirePages(pagesNeeded);
    }
    if (pages === null) {
      this.pool._noteRejected(this);
      return null;
    }
    const allocation = new Allocation(this, bytes, pages);
    this._allocations.add(allocation);
    this._bytesHeld += bytes;
    this._pagesHeld += pagesNeeded;
    this.pool._noteAcquire(this, bytes, pagesNeeded);
    return allocation;
  }

  acquireOrThrow(bytes) {
    const allocation = this.acquire(bytes);
    if (allocation === null) {
      throw new Error(
        `allocation of ${bytes} bytes rejected for operator "${this.operatorId}": memory budget exhausted`
      );
    }
    return allocation;
  }

  release(allocation) {
    if (allocation.released) return;
    if (!this._allocations.has(allocation)) {
      throw new Error('allocation does not belong to this lease');
    }
    allocation.released = true;
    this._allocations.delete(allocation);
    this._bytesHeld -= allocation.bytes;
    this._pagesHeld -= allocation.pages.length;
    this.pool._releasePages(allocation.pages);
    this.pool._noteRelease(this, allocation.bytes, allocation.pages.length);
  }

  releaseAll() {
    for (const allocation of [...this._allocations]) {
      this.release(allocation);
    }
  }

  /** Close the lease and return every page to the pool. Idempotent. */
  close() {
    if (!this.open) return;
    this.releaseAll();
    this.open = false;
    this.pool._leases.delete(this);
  }
}

class MemoryPool {
  constructor({ pageSize, maxArenaBytes }) {
    if (!Number.isInteger(pageSize) || pageSize <= 0) {
      throw new RangeError('pageSize must be a positive integer');
    }
    if (!Number.isInteger(maxArenaBytes) || maxArenaBytes < pageSize) {
      throw new RangeError('maxArenaBytes must be an integer >= pageSize');
    }
    this.pageSize = pageSize;
    this.maxArenaBytes = maxArenaBytes;
    this.maxPages = Math.floor(maxArenaBytes / pageSize);
    this._freePages = [];
    this._pagesInUse = 0;
    this._leases = new Set();
    this._operators = new Map();
    this._stats = {
      peakBytes: 0,
      spillCount: 0,
      rejectedCount: 0,
      allocationCount: 0,
    };
  }

  lease(options = {}) {
    if (options.operatorId == null) throw new TypeError('lease requires an operatorId');
    const lease = new Lease(this, options);
    this._leases.add(lease);
    this._operatorStats(lease.operatorId);
    return lease;
  }

  /** Cancel a query: close every lease that belongs to it. */
  cancelQuery(queryId) {
    let closed = 0;
    for (const lease of [...this._leases]) {
      if (lease.queryId === queryId) {
        lease.close();
        closed += 1;
      }
    }
    return closed;
  }

  get reservedBytes() {
    return this._pagesInUse * this.pageSize;
  }

  get fragmentationBytes() {
    let total = 0;
    for (const lease of this._leases) total += lease.fragmentationBytes;
    return total;
  }

  stats() {
    const operators = {};
    for (const [id, s] of this._operators) {
      operators[id] = { ...s };
    }
    return {
      pageSize: this.pageSize,
      maxArenaBytes: this.maxArenaBytes,
      maxPages: this.maxPages,
      reservedBytes: this.reservedBytes,
      reservedPages: this._pagesInUse,
      cachedFreePages: this._freePages.length,
      fragmentationBytes: this.fragmentationBytes,
      peakBytes: this._stats.peakBytes,
      spillCount: this._stats.spillCount,
      rejectedCount: this._stats.rejectedCount,
      allocationCount: this._stats.allocationCount,
      operators,
    };
  }

  _operatorStats(operatorId) {
    let s = this._operators.get(operatorId);
    if (!s) {
      s = { currentBytes: 0, peakBytes: 0, reservedBytes: 0, fragmentationBytes: 0, spills: 0, rejections: 0 };
      this._operators.set(operatorId, s);
    }
    return s;
  }

  _tryAcquirePages(count) {
    if (this._pagesInUse + count > this.maxPages) return null;
    const pages = [];
    for (let i = 0; i < count; i++) {
      pages.push(this._freePages.pop() ?? Buffer.alloc(this.pageSize));
    }
    this._pagesInUse += count;
    return pages;
  }

  _releasePages(pages) {
    this._pagesInUse -= pages.length;
    for (const page of pages) this._freePages.push(page);
  }

  /**
   * Fair spill policy: ask the largest consumers first (measured in pages
   * held) to spill, until enough room exists or nobody can spill any more.
   */
  _requestSpill(pagesNeeded) {
    const shortBy = () => this._pagesInUse + pagesNeeded - this.maxPages;
    while (shortBy() > 0) {
      const candidates = [...this._leases]
        .filter((l) => l.open && l._pagesHeld > 0 && typeof l.onSpill === 'function')
        .sort((a, b) => b._pagesHeld - a._pagesHeld);
      let progressed = false;
      for (const candidate of candidates) {
        if (shortBy() <= 0) break;
        const freedPages = candidate.onSpill(candidate, { pagesNeeded: shortBy(), pool: this }) | 0;
        if (freedPages > 0) {
          this._stats.spillCount += 1;
          this._operatorStats(candidate.operatorId).spills += 1;
          progressed = true;
        }
      }
      if (!progressed) break;
    }
  }

  _noteAcquire(lease, bytes, pages) {
    this._stats.allocationCount += 1;
    const reserved = this.reservedBytes;
    if (reserved > this._stats.peakBytes) this._stats.peakBytes = reserved;
    const s = this._operatorStats(lease.operatorId);
    s.currentBytes += bytes;
    s.reservedBytes += pages * this.pageSize;
    s.fragmentationBytes = s.reservedBytes - s.currentBytes;
    if (s.currentBytes > s.peakBytes) s.peakBytes = s.currentBytes;
  }

  _noteRelease(lease, bytes, pages) {
    const s = this._operatorStats(lease.operatorId);
    s.currentBytes -= bytes;
    s.reservedBytes -= pages * this.pageSize;
    s.fragmentationBytes = s.reservedBytes - s.currentBytes;
  }

  _noteRejected(lease) {
    this._stats.rejectedCount += 1;
    this._operatorStats(lease.operatorId).rejections += 1;
  }
}

module.exports = { MemoryPool, Lease, Allocation };
