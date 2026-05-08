/**
 * Unit tests for wireExitOnComplete: verifies the orchestrator-completion →
 * SIGTERM wiring used in main.ts. The helper is pure (no infra), so this is a
 * lightweight test even though it lives under tests/integration/ for
 * collocation with the rest of the suite.
 */
import { describe, it, expect, vi } from 'vitest';
import pino from 'pino';
import { wireExitOnComplete } from '../../src/runtime/exit-on-complete.ts';

const silentLogger = pino({ level: 'silent' });

// Two ticks: one for the runPromise's .then() callback, one for the test continuation.
// Deterministic, unlike a setTimeout-based flush.
const flushMicrotasks = async () => {
    await Promise.resolve();
    await Promise.resolve();
};

describe('wireExitOnComplete', () => {
    it('does not call kill when disabled, even if the run promise resolves', async () => {
        const kill = vi.fn();
        wireExitOnComplete(Promise.resolve(), false, silentLogger, kill, 12345);
        await flushMicrotasks();
        expect(kill).not.toHaveBeenCalled();
    });

    it('sends SIGTERM and logs when enabled and the run promise resolves', async () => {
        const kill = vi.fn();
        const logSpy = vi.spyOn(silentLogger, 'info');
        wireExitOnComplete(Promise.resolve(), true, silentLogger, kill, 12345);
        await flushMicrotasks();
        expect(kill).toHaveBeenCalledTimes(1);
        expect(kill).toHaveBeenCalledWith(12345, 'SIGTERM');
        expect(logSpy).toHaveBeenCalledWith(
            'Orchestrator completed; exit-on-complete set, signaling shutdown',
        );
        logSpy.mockRestore();
    });

    it('does not call kill if enabled but the run promise rejects', async () => {
        const kill = vi.fn();
        const rejected = Promise.reject(new Error('orchestrator crashed'));
        rejected.catch(() => {});
        wireExitOnComplete(rejected, true, silentLogger, kill, 12345);
        await flushMicrotasks();
        expect(kill).not.toHaveBeenCalled();
    });
});
