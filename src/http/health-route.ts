import type { FastifyInstance } from 'fastify';
import type { RunnerStatus } from '../runtime/batch-runner.ts';

export interface HealthDeps {
  mongoReader: { isConnected(): boolean };
  chWriter: { isConnected(): boolean };
  redisState: { isHealthy(): Promise<boolean> };
  manifestStore: { isWritable(): Promise<boolean> };
  orchestrator: { getStatus(): RunnerStatus };
}

export function registerHealthRoutes(app: FastifyInstance, deps: HealthDeps): void {
  const { mongoReader, chWriter, redisState, manifestStore, orchestrator } = deps;

  // GET /healthz - simple liveness (always 200 if server is up)
  app.get('/healthz', async (_request, reply) => {
    return reply.status(200).send({ status: 'alive' });
  });

  // GET /readyz - readiness check
  app.get('/readyz', async (_request, reply) => {
    const checks: Record<string, boolean> = {
      mongo: false,
      clickhouse: false,
      redis: false,
      manifestStore: false,
      batchRunner: false,
    };

    checks.mongo = mongoReader.isConnected();
    checks.clickhouse = chWriter.isConnected();

    try {
      checks.redis = await redisState.isHealthy();
    } catch {
      checks.redis = false;
    }

    try {
      checks.manifestStore = await manifestStore.isWritable();
    } catch {
      checks.manifestStore = false;
    }
    checks.batchRunner = orchestrator.getStatus() !== 'failed';

    const allHealthy = Object.values(checks).every(Boolean);

    return reply.status(allHealthy ? 200 : 503).send({
      ready: allHealthy,
      checks,
    });
  });
}
