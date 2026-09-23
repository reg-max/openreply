/**
 * Serverless job runner (BullMQ/Redis replacement).
 *
 * Upstream OpenReply pushes every comment, postback, message and follow-up onto
 * a BullMQ queue that a long-lived worker drains. This fork has no worker and no
 * Redis, so:
 *
 *   - work with no delay runs inline, inside the request that produced it
 *     (the Meta webhook, or the cron sweep);
 *   - work with a delay (rate-limit backoff, follow-up messages, the opening-DM
 *     read fallback) is persisted as a DeferredJob row and run by the next cron
 *     sweep that finds it due;
 *   - a job that throws is persisted the same way and retried by a later sweep
 *     with a growing backoff, so a transient Instagram error does not lose a DM.
 *
 * Duplicate protection does not depend on this module: processComment and its
 * siblings already key on DmLog (automationId + commentId), so running the same
 * comment twice is a no-op rather than a second DM. `jobId` is kept as a
 * dedupeKey so the same delayed job is not scheduled twice.
 */

import { Prisma } from "@/app/generated/prisma/client";
import { prisma } from "@/lib/db/client";
import type { DmQueueJob } from "@/lib/queue/client";

export interface EnqueueOptions {
  /** Milliseconds to wait before running. Omitted or 0 runs inline. */
  delay?: number;
  /** Stable id used to avoid scheduling the same delayed job twice. */
  jobId?: string;
}

const MAX_ATTEMPTS = 5;
const RETRY_BACKOFF_MS = [
  60_000, // 1 min
  5 * 60_000,
  15 * 60_000,
  60 * 60_000,
];
// A job claimed by a sweep that then died is considered abandoned after this.
const LOCK_TIMEOUT_MS = 10 * 60_000;

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}

/**
 * dispatchJob wraps failures it knows are pointless to repeat (an unconfirmed
 * Zernio delivery, for one) in BullMQ's UnrecoverableError. Retrying those
 * risks a duplicate DM, so they are recorded and dropped instead.
 */
function isUnrecoverable(error: unknown): boolean {
  return error instanceof Error && error.name === "UnrecoverableError";
}

/**
 * Run one job in this process. Imported lazily because dm-worker.ts enqueues
 * through this module, and a static import both ways would be a cycle.
 */
async function runJob(
  name: string,
  data: DmQueueJob,
  jobId: string | undefined,
  attemptsMade: number
): Promise<void> {
  const { runDmJobInline } = await import("@/lib/queue/dm-worker");
  await runDmJobInline({ name, data, id: jobId, attemptsMade });
}

async function scheduleDeferred({
  name,
  data,
  jobId,
  runAt,
  attempts,
}: {
  name: string;
  data: DmQueueJob;
  jobId?: string;
  runAt: Date;
  attempts: number;
}): Promise<void> {
  try {
    await prisma.deferredJob.create({
      data: {
        name,
        dedupeKey: jobId ?? null,
        data: data as unknown as Prisma.InputJsonValue,
        runAt,
        attempts,
      },
    });
  } catch (error) {
    // Unique violation on dedupeKey: this job is already scheduled, which is
    // exactly what the caller wanted.
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === "P2002"
    ) {
      return;
    }
    throw error;
  }
}

/**
 * Enqueue work. Runs it now unless a delay was requested.
 *
 * A job that fails inline is not thrown back at the caller: the Meta webhook
 * must answer 200 or Meta retries the whole delivery, and the reconciler sweep
 * should not abort its remaining campaigns because one comment failed. The job
 * is persisted for retry instead, and the failure is recorded.
 */
export async function enqueueJob(
  name: string,
  data: DmQueueJob,
  options: EnqueueOptions = {}
): Promise<void> {
  const { delay = 0, jobId } = options;

  if (delay > 0) {
    await scheduleDeferred({
      name,
      data,
      jobId,
      runAt: new Date(Date.now() + delay),
      attempts: 0,
    });
    return;
  }

  try {
    await runJob(name, data, jobId, 0);
  } catch (error) {
    const message = formatError(error);
    if (isUnrecoverable(error)) {
      console.error(`[Inline] Job ${jobId ?? name} unrecoverable:`, message);
      return;
    }
    console.error(`[Inline] Job ${jobId ?? name} failed:`, message);

    await prisma.operationalEvent
      .create({
        data: {
          source: "WORKER",
          level: "WARNING",
          message: `Inline job ${jobId ?? name} failed, retry scheduled: ${message}`,
          payload: { jobId: jobId ?? null, name },
        },
      })
      .catch(() => {});

    await scheduleDeferred({
      name,
      data,
      jobId: jobId ? `${jobId}_retry` : undefined,
      runAt: new Date(Date.now() + RETRY_BACKOFF_MS[0]),
      attempts: 1,
    }).catch((scheduleError) => {
      console.error(
        `[Inline] Could not schedule retry for ${jobId ?? name}:`,
        formatError(scheduleError)
      );
    });
  }
}

export interface DeferredRunSummary {
  ran: number;
  failed: number;
  dropped: number;
}

/**
 * Run every deferred job that is due. Called by the cron sweep.
 *
 * Each row is claimed with a conditional update before running, so two sweeps
 * that overlap cannot both run the same job. A job that keeps failing is
 * dropped after MAX_ATTEMPTS and recorded, rather than retried forever.
 */
export async function runDueDeferredJobs(
  limit = 25
): Promise<DeferredRunSummary> {
  const now = new Date();
  const lockCutoff = new Date(now.getTime() - LOCK_TIMEOUT_MS);

  const due = await prisma.deferredJob.findMany({
    where: {
      runAt: { lte: now },
      OR: [{ lockedAt: null }, { lockedAt: { lt: lockCutoff } }],
    },
    orderBy: { runAt: "asc" },
    take: limit,
  });

  const summary: DeferredRunSummary = { ran: 0, failed: 0, dropped: 0 };

  for (const job of due) {
    const claimed = await prisma.deferredJob.updateMany({
      where: {
        id: job.id,
        OR: [{ lockedAt: null }, { lockedAt: { lt: lockCutoff } }],
      },
      data: { lockedAt: new Date() },
    });
    if (claimed.count === 0) continue;

    try {
      await runJob(
        job.name,
        job.data as unknown as DmQueueJob,
        job.dedupeKey ?? undefined,
        job.attempts
      );
      await prisma.deferredJob.delete({ where: { id: job.id } });
      summary.ran += 1;
    } catch (error) {
      const message = formatError(error);
      const attempts = job.attempts + 1;

      if (isUnrecoverable(error) || attempts >= MAX_ATTEMPTS) {
        await prisma.deferredJob.delete({ where: { id: job.id } }).catch(() => {});
        await prisma.operationalEvent
          .create({
            data: {
              source: "WORKER",
              level: "ERROR",
              message: `Deferred job ${job.name} dropped after ${attempts} attempts: ${message}`,
              payload: { jobId: job.dedupeKey, name: job.name, attempts },
            },
          })
          .catch(() => {});
        summary.dropped += 1;
        continue;
      }

      const backoff =
        RETRY_BACKOFF_MS[Math.min(attempts - 1, RETRY_BACKOFF_MS.length - 1)];

      await prisma.deferredJob.update({
        where: { id: job.id },
        data: {
          attempts,
          lockedAt: null,
          runAt: new Date(Date.now() + backoff),
        },
      });
      summary.failed += 1;
    }
  }

  return summary;
}

/** Counts for the diagnostics and health endpoints. */
export async function getDeferredJobCounts(): Promise<{
  waiting: number;
  delayed: number;
  retrying: number;
}> {
  const now = new Date();
  const [waiting, delayed, retrying] = await Promise.all([
    prisma.deferredJob.count({ where: { runAt: { lte: now } } }),
    prisma.deferredJob.count({ where: { runAt: { gt: now } } }),
    prisma.deferredJob.count({ where: { attempts: { gt: 0 } } }),
  ]);
  return { waiting, delayed, retrying };
}
