import type { Logger } from "pino";
import { PerformanceObserver } from "node:perf_hooks";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type GcMode = "after-batch" | "now" | "force";

export interface GcConfig {
  enabled: boolean;
  rssSoftLimitBytes: number;
  rssHardLimitBytes: number;
  heapUsedRatio: number;
  everyNBatches: number;
}

export interface GcEvent {
  kind: number | null;
  durationMs: number;
  startTimeMs: number;
  timestamp: string;
}

export interface GcTelemetry {
  gcAvailable: boolean;
  gcState: "idle" | "pending" | "running";
  lastGcReason: string | null;
  lastGcDurationMs: number;
  heapUsedBefore: number;
  heapUsedAfter: number;
  rssBefore: number;
  rssAfter: number;
  gcCountTotal: number;
  observedGcCount: number;
  lastObservedGcDurationMs: number;
  lastObservedGcKind: number | null;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Wraps `setImmediate` in a promise so we can `await` one full I/O cycle.
 */
function nextImmediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

// ---------------------------------------------------------------------------
// GcController
// ---------------------------------------------------------------------------

/**
 * Manual GC controller.
 *
 * Requires Node to be started with `--expose-gc` so that `global.gc` is
 * available.  When GC is unavailable the controller is a safe no-op; it
 * records that fact in its telemetry so operators can tell from metrics.
 */
export class GcController {
  private readonly config: GcConfig;
  private readonly logger: Logger;

  private state: "idle" | "pending" | "running" = "idle";
  private pendingReason: string | null = null;
  private gcCount = 0;
  private lastReason: string | null = null;
  private lastDurationMs = 0;
  private lastHeapUsedBefore = 0;
  private lastHeapUsedAfter = 0;
  private lastRssBefore = 0;
  private lastRssAfter = 0;

  /** PerformanceObserver tracking V8-initiated GC events. */
  private gcObserver: PerformanceObserver | null = null;
  private lastObservedEvent: GcEvent | null = null;
  private observedGcCount = 0;

  constructor(config: GcConfig, logger: Logger) {
    this.config = config;
    this.logger = logger.child({ component: "GcController" });

    if (!this.isAvailable) {
      this.logger.warn(
        "global.gc is not available. Start Node with --expose-gc to enable manual GC.",
      );
    }
  }

  /**
   * Start observing V8 GC events via PerformanceObserver.
   * Call this after construction when the event loop is ready.
   */
  start(): void {
    if (this.gcObserver) return;

    this.gcObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const detail = entry as unknown as { detail?: { kind?: number }; kind?: number; duration: number; startTime: number };
        const kind: number | null = detail.detail?.kind ?? detail.kind ?? null;

        this.lastObservedEvent = {
          kind,
          durationMs: entry.duration,
          startTimeMs: entry.startTime,
          timestamp: new Date().toISOString(),
        };
        this.observedGcCount++;
      }
    });

    try {
      this.gcObserver.observe({ entryTypes: ["gc"] });
    } catch {
      this.logger.debug("PerformanceObserver for GC entryType not supported in this runtime");
    }
  }

  // -----------------------------------------------------------------------
  // Accessors
  // -----------------------------------------------------------------------

  /** Check if GC is available (`global.gc` exists). */
  get isAvailable(): boolean {
    return typeof global.gc === "function";
  }

  /** Check if GC has been marked as pending. */
  get isPending(): boolean {
    return this.state === "pending";
  }

  // -----------------------------------------------------------------------
  // Telemetry
  // -----------------------------------------------------------------------

  /** Return a snapshot of the current GC telemetry. */
  getTelemetry(): GcTelemetry {
    const lastEvent = this.lastObservedEvent;
    return {
      gcAvailable: this.isAvailable,
      gcState: this.state,
      lastGcReason: this.lastReason,
      lastGcDurationMs: this.lastDurationMs,
      heapUsedBefore: this.lastHeapUsedBefore,
      heapUsedAfter: this.lastHeapUsedAfter,
      rssBefore: this.lastRssBefore,
      rssAfter: this.lastRssAfter,
      gcCountTotal: this.gcCount,
      observedGcCount: this.observedGcCount,
      lastObservedGcDurationMs: lastEvent?.durationMs ?? 0,
      lastObservedGcKind: lastEvent?.kind ?? null,
    };
  }

  // -----------------------------------------------------------------------
  // Decision helpers
  // -----------------------------------------------------------------------

  /**
   * Determine whether GC should run after the given batch sequence number.
   *
   * Checks three independent conditions (any one triggers a `true`):
   *   1. `heapUsed / heapTotal >= heapUsedRatio`
   *   2. `rss >= rssSoftLimitBytes`
   *   3. `batchSeq % everyNBatches === 0`
   */
  shouldRunAfterBatch(batchSeq: number): boolean {
    if (!this.config.enabled) return false;

    const mem = process.memoryUsage();
    const heapRatio = mem.heapTotal > 0 ? mem.heapUsed / mem.heapTotal : 0;

    if (heapRatio >= this.config.heapUsedRatio) return true;
    if (mem.rss >= this.config.rssSoftLimitBytes) return true;
    if (this.config.everyNBatches > 0 && batchSeq % this.config.everyNBatches === 0) return true;

    return false;
  }

  // -----------------------------------------------------------------------
  // Execution
  // -----------------------------------------------------------------------

  /**
   * Mark GC as pending. The actual collection will happen when the current
   * batch completes and the runner calls {@link runGc}.
   */
  markPending(reason: string): void {
    if (this.state === "idle") {
      this.state = "pending";
      this.pendingReason = reason;
      this.logger.debug({ reason }, "GC marked pending");
    }
  }

  /**
   * Execute garbage collection.
   *
   * @param mode   - Controls preconditions:
   *   - `after-batch`: normal conditional GC (obeys thresholds).
   *   - `now`: only if currently idle (skip if already running).
   *   - `force`: bypass threshold checks, always collect.
   * @param reason - Human-readable reason for log messages.
   * @returns `true` if GC actually ran, `false` if skipped.
   */
  async runGc(mode: GcMode, reason: string): Promise<boolean> {
    // Guard: GC not available
    if (!this.isAvailable) {
      this.logger.debug("Skipping GC: global.gc not available");
      return false;
    }

    // Guard: not enabled (unless forced)
    if (!this.config.enabled && mode !== "force") {
      return false;
    }

    // Guard: already running
    if (this.state === "running") {
      this.logger.debug("Skipping GC: already running");
      return false;
    }

    // Mode-specific guards
    if (mode === "now" && this.state !== "idle") {
      this.logger.debug({ state: this.state }, "Skipping GC (mode=now): not idle");
      return false;
    }

    // ------------------------------------------------------------------
    // Proceed with collection
    // ------------------------------------------------------------------

    this.state = "running";
    this.lastReason = reason;

    // 1. Await one microtask to let pending promises settle
    await Promise.resolve();

    // 2. Await one setImmediate to drain the I/O queue
    await nextImmediate();

    // 3. Record memory before
    const memBefore = process.memoryUsage();
    this.lastHeapUsedBefore = memBefore.heapUsed;
    this.lastRssBefore = memBefore.rss;

    // 4. Run GC
    const startMs = performance.now();
    global.gc!();
    const elapsed = performance.now() - startMs;
    this.lastDurationMs = Math.round(elapsed * 100) / 100;

    // 5. Record memory after
    const memAfter = process.memoryUsage();
    this.lastHeapUsedAfter = memAfter.heapUsed;
    this.lastRssAfter = memAfter.rss;

    // 6. Update counters & state
    this.gcCount++;
    this.state = "idle";
    this.pendingReason = null;

    const freedMb = ((memBefore.heapUsed - memAfter.heapUsed) / 1024 / 1024).toFixed(1);

    this.logger.info(
      {
        reason,
        mode,
        durationMs: this.lastDurationMs,
        heapUsedBeforeMb: (memBefore.heapUsed / 1024 / 1024).toFixed(1),
        heapUsedAfterMb: (memAfter.heapUsed / 1024 / 1024).toFixed(1),
        freedMb,
        rssMb: (memAfter.rss / 1024 / 1024).toFixed(1),
        gcCountTotal: this.gcCount,
      },
      "GC completed",
    );

    // Warn if RSS exceeds hard limit after collection
    if (memAfter.rss >= this.config.rssHardLimitBytes) {
      this.logger.warn(
        {
          rssBytes: memAfter.rss,
          rssHardLimitBytes: this.config.rssHardLimitBytes,
        },
        "RSS exceeds hard limit even after GC",
      );
    }

    return true;
  }

  // -----------------------------------------------------------------------
  // Cleanup
  // -----------------------------------------------------------------------

  /** Disconnect the PerformanceObserver and release resources. */
  dispose(): void {
    try {
      this.gcObserver?.disconnect();
      this.gcObserver = null;
    } catch {
      // observer may already be disconnected
    }
  }
}
