import { monitorEventLoopDelay, type IntervalHistogram } from "node:perf_hooks";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ProcessMetricsSnapshot {
  rssBytes: number;
  heapTotalBytes: number;
  heapUsedBytes: number;
  externalBytes: number;
  arrayBuffersBytes: number;
  eventLoopLagMs_p95_1m: number;
  cpuUserSec: number;
  cpuSystemSec: number;
}

// ---------------------------------------------------------------------------
// ProcessMetricsCollector
// ---------------------------------------------------------------------------

/**
 * Collects process-level metrics:
 *
 * - Memory usage via `process.memoryUsage()`
 * - CPU time via `process.cpuUsage()`
 * - Event loop lag via `perf_hooks.monitorEventLoopDelay()`
 *
 * The event loop lag histogram is sampled at 20 ms resolution and the
 * collector resets it every 60 seconds (the "1m" window). The p95 value
 * returned in a snapshot is always from the most recently completed window
 * so that callers see a stable, non-zero value rather than a histogram
 * that was just reset.
 */
export class ProcessMetricsCollector {
  private histogram: IntervalHistogram | null = null;
  private lastP95Ns = 0;
  private running = false;
  private resetTimer: ReturnType<typeof setInterval> | null = null;

  /** Resolution for the event loop delay histogram (20 ms). */
  private static readonly ELD_RESOLUTION_MS = 20;
  /** Window duration before the histogram resets (60 s). */
  private static readonly ELD_WINDOW_MS = 60_000;

  constructor() {
    // Histogram is created lazily in start() so the collector can be
    // instantiated before the event loop is fully warmed up.
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /** Start collecting event loop lag samples. */
  start(): void {
    if (this.running) return;

    this.histogram = monitorEventLoopDelay({
      resolution: ProcessMetricsCollector.ELD_RESOLUTION_MS,
    });
    this.histogram.enable();
    this.running = true;

    // Rotate the histogram every 60 s to keep a sliding 1 m window.
    this.resetTimer = setInterval(() => {
      if (this.histogram) {
        // Capture p95 before resetting so snapshot() has a value.
        this.lastP95Ns = this.histogram.percentile(95);
        this.histogram.reset();
      }
    }, ProcessMetricsCollector.ELD_WINDOW_MS);

    // Allow the process to exit even if the timer is still alive.
    if (this.resetTimer && typeof this.resetTimer.unref === "function") {
      this.resetTimer.unref();
    }
  }

  /** Stop collecting event loop lag samples. */
  stop(): void {
    if (!this.running) return;

    if (this.histogram) {
      // Capture final p95 before disabling.
      this.lastP95Ns = this.histogram.percentile(95);
      this.histogram.disable();
      this.histogram = null;
    }

    if (this.resetTimer !== null) {
      clearInterval(this.resetTimer);
      this.resetTimer = null;
    }

    this.running = false;
  }

  // -----------------------------------------------------------------------
  // Snapshot
  // -----------------------------------------------------------------------

  /**
   * Return a point-in-time snapshot of process metrics.
   *
   * Memory and CPU values are instantaneous; the event loop lag p95 is
   * from the most recently completed 60 s window (or the current window
   * if no rotation has happened yet).
   */
  snapshot(): ProcessMetricsSnapshot {
    const mem = process.memoryUsage();
    const cpu = process.cpuUsage();

    // p95 event loop lag: prefer the live histogram if it has samples,
    // fall back to the last captured value from the most recent reset.
    let p95Ns = this.lastP95Ns;
    if (this.histogram) {
      const liveP95 = this.histogram.percentile(95);
      if (liveP95 > 0) {
        p95Ns = liveP95;
      }
    }

    return {
      rssBytes: mem.rss,
      heapTotalBytes: mem.heapTotal,
      heapUsedBytes: mem.heapUsed,
      externalBytes: mem.external,
      arrayBuffersBytes: mem.arrayBuffers ?? 0,
      // Convert nanoseconds to milliseconds
      eventLoopLagMs_p95_1m: p95Ns / 1e6,
      // Convert microseconds to seconds
      cpuUserSec: cpu.user / 1e6,
      cpuSystemSec: cpu.system / 1e6,
    };
  }
}
