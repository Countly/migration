import type { FastifyInstance } from 'fastify';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcMode } from '../runtime/gc-controller.ts';

export interface ControlDeps {
  orchestrator: {
    pause(): void;
    resume(): void;
    stopAfterBatch(): void;
    getStatus(): RunnerStatus;
  };
  gcController: {
    runGc(mode: GcMode, reason: string): Promise<boolean>;
    isAvailable: boolean;
  };
}

interface GcRequestBody {
  mode: GcMode;
}

export function registerControlRoutes(app: FastifyInstance, deps: ControlDeps): void {
  const { orchestrator, gcController } = deps;

  // POST /control/pause - pause after current batch
  app.post('/control/pause', async (_request, reply) => {
    orchestrator.pause();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/resume - resume batch scheduling
  app.post('/control/resume', async (_request, reply) => {
    orchestrator.resume();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/stop-after-batch - stop cleanly after current batch
  app.post('/control/stop-after-batch', async (_request, reply) => {
    orchestrator.stopAfterBatch();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/gc - trigger manual GC
  app.post<{ Body: GcRequestBody }>('/control/gc', async (request, reply) => {
    if (!gcController.isAvailable) {
      return reply.status(400).send({
        ok: false,
        error: 'GC not available (--expose-gc not set)',
      });
    }

    const { mode } = request.body ?? { mode: 'now' };
    const triggered = await gcController.runGc(mode, 'manual-http-request');

    return reply.status(200).send({
      ok: true,
      mode,
      triggered,
    });
  });
}
