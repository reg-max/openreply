/**
 * Local sweep runner.
 *
 * This fork has no queue worker: in production an external cron calls
 * /api/cron/sweep. This script exists so the same work can be run locally
 * (`npm run worker`) against a dev database — it attaches pending next-reel
 * campaigns, reconciles comments webhooks missed, and runs deferred jobs that
 * have come due, on a loop.
 */

import os from "node:os";
import { attachPendingNextReels } from "@/lib/automation/attach-next-reel";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";
import { reconcileComments } from "@/lib/polling/comment-reconciler";
import { runDueDeferredJobs } from "@/lib/queue/inline";

const startedAt = new Date().toISOString();
const POLL_INTERVAL_MS = Number(
  process.env.COMMENT_POLL_INTERVAL_MS ?? 5 * 60_000
);

console.log("[Sweep] Started");

async function sweep() {
  try {
    const attached = await attachPendingNextReels();
    if (attached.bound > 0 || attached.failedAccounts > 0) {
      console.log("[Sweep] Next-reel attachment:", attached);
    }
    await reconcileComments();
    const deferred = await runDueDeferredJobs();
    if (deferred.ran > 0 || deferred.failed > 0 || deferred.dropped > 0) {
      console.log("[Sweep] Deferred jobs:", deferred);
    }
    await recordWorkerHeartbeat({
      pid: process.pid,
      hostname: os.hostname(),
      startedAt,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[Sweep] Failed:", message);
  }
}

void sweep();
const timer = setInterval(() => void sweep(), POLL_INTERVAL_MS);

function shutdown(signal: string) {
  console.log(`[Sweep] ${signal} received, stopping`);
  clearInterval(timer);
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
