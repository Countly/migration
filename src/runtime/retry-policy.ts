import { setTimeout as sleep } from "node:timers/promises";
import type { Logger } from "pino";

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

// ---------------------------------------------------------------------------
// RetryPolicy
// ---------------------------------------------------------------------------

export class RetryPolicy {
  private readonly config: RetryConfig;

  get maxRetries(): number {
    return this.config.maxRetries;
  }

  constructor(config: RetryConfig) {
    this.config = config;
  }

  /**
   * Calculate delay for a given attempt (0-indexed) with jitter.
   *
   * Uses exponential backoff capped at `maxDelayMs`, then adds 0-25%
   * random jitter on top of the capped value to avoid thundering-herd
   * effects across concurrent workers.
   */
  getDelay(attempt: number): number {
    const exponential = this.config.baseDelayMs * Math.pow(2, attempt);
    const capped = Math.min(exponential, this.config.maxDelayMs);
    // Add 0-25% jitter
    const jitter = capped * Math.random() * 0.25;
    return capped + jitter;
  }

  /**
   * Returns `true` when the caller may attempt another retry.
   * `attempt` is 0-indexed (the first retry is attempt 1).
   */
  shouldRetry(attempt: number): boolean {
    return attempt < this.config.maxRetries;
  }

  /**
   * Execute an async function with exponential-backoff retries.
   *
   * On each failure the delay is computed via {@link getDelay} and logged at
   * `warn` level.  After `maxRetries` consecutive failures the last error is
   * re-thrown so callers can handle it (e.g. mark a batch as failed).
   *
   * @param fn    - The async operation to attempt.
   * @param label - A human-readable label for log messages.
   * @param logger - Pino logger instance.
   * @returns The resolved value of `fn` on the first successful attempt.
   */
  async execute<T>(
    fn: () => Promise<T>,
    label: string,
    logger: Logger,
    onError?: (attempt: number, error: Error) => Promise<void>,
  ): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      try {
        return await fn();
      } catch (err) {
        lastError = toError(err);

        if (onError) {
          await onError(attempt, lastError).catch(() => {});
        }

        if (this.shouldRetry(attempt + 1)) {
          const delay = this.getDelay(attempt);
          logger.warn(
            {
              label,
              attempt: attempt + 1,
              maxRetries: this.config.maxRetries,
              delayMs: Math.round(delay),
              error: lastError.message,
            },
            "Retryable operation failed, backing off",
          );
          await sleep(delay);
        }
      }
    }

    logger.error(
      {
        label,
        maxRetries: this.config.maxRetries,
        error: lastError?.message,
      },
      "All retries exhausted",
    );

    throw lastError;
  }
}
