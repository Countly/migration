import type pino from 'pino';

/**
 * Wires orchestrator completion to a SIGTERM on the current process when the
 * EXIT_ON_COMPLETE flag is set. SIGTERM (rather than calling shutdown()
 * directly) reuses the existing graceful-shutdown handler in main.ts, which
 * already drains the async writer, releases locks, and exits 0 on its own
 * timeout budget.
 *
 * `kill` and `pid` are injectable so the helper is testable without spawning
 * a subprocess.
 */
export function wireExitOnComplete(
    runPromise: Promise<unknown>,
    enabled: boolean,
    logger: pino.Logger,
    kill: (pid: number, signal: NodeJS.Signals) => void = process.kill.bind(process),
    pid: number = process.pid,
): void {
    if (!enabled) return;
    runPromise.then(
        () => {
            logger.info('Orchestrator completed; exit-on-complete set, signaling shutdown');
            kill(pid, 'SIGTERM');
        },
        () => {
            // Run promise rejected — main.ts's existing .catch path handles fatal exit.
        },
    );
}
