/**
 * Health and alerts for the DM pipeline.
 *
 * In this fork there is no long-lived worker: the heartbeat is written by the
 * cron sweep (/api/cron/sweep) each time it runs, and stored in Postgres rather
 * than Redis. The TTL is therefore sized for a sweep cadence of a few minutes,
 * not for a 30-second worker heartbeat.
 */

import { kvGetJson, kvPushCapped, kvReadList, kvSetJson } from "@/lib/kv/pg-kv";

const WORKER_HEALTH_KEY = "health:worker:dm";
const WORKER_ALERTS_KEY = "alerts:worker:dm";
// A sweep runs every ~10 minutes, so allow two missed runs plus slack before
// calling the pipeline unhealthy.
const WORKER_HEARTBEAT_TTL_SECONDS = 30 * 60;
const WORKER_ALERTS_CAP = 25;

export interface WorkerHeartbeat {
  status: "running";
  worker: "dm";
  pid: number;
  hostname?: string;
  startedAt?: string;
  checkedAt: string;
}

export interface WorkerHealth {
  healthy: boolean;
  heartbeat: WorkerHeartbeat | null;
  ageMs: number | null;
}

export interface WorkerAlert {
  level: "warning" | "error";
  message: string;
  jobId?: string;
  instagramAccountId?: string;
  commentId?: string;
  createdAt: string;
}

export async function recordWorkerHeartbeat(
  heartbeat: Omit<WorkerHeartbeat, "checkedAt" | "status" | "worker">
) {
  const payload: WorkerHeartbeat = {
    ...heartbeat,
    status: "running",
    worker: "dm",
    checkedAt: new Date().toISOString(),
  };

  await kvSetJson(WORKER_HEALTH_KEY, payload, WORKER_HEARTBEAT_TTL_SECONDS);
}

export async function getWorkerHealth(): Promise<WorkerHealth> {
  const heartbeat = await kvGetJson<WorkerHeartbeat>(WORKER_HEALTH_KEY);

  if (!heartbeat) {
    return { healthy: false, heartbeat: null, ageMs: null };
  }

  const ageMs = Date.now() - new Date(heartbeat.checkedAt).getTime();
  return {
    healthy: ageMs <= WORKER_HEARTBEAT_TTL_SECONDS * 1000,
    heartbeat,
    ageMs,
  };
}

export async function recordWorkerAlert(alert: Omit<WorkerAlert, "createdAt">) {
  const payload: WorkerAlert = {
    ...alert,
    createdAt: new Date().toISOString(),
  };

  await kvPushCapped(WORKER_ALERTS_KEY, payload, WORKER_ALERTS_CAP);
}

export async function getWorkerAlerts(limit = 10): Promise<WorkerAlert[]> {
  return kvReadList<WorkerAlert>(WORKER_ALERTS_KEY, limit);
}

export { WORKER_HEARTBEAT_TTL_SECONDS };
