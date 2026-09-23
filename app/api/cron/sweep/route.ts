/**
 * Comment sweep — the replacement for the long-lived DM worker.
 *
 * Upstream OpenReply runs a Node worker that, every few minutes, attaches
 * pending next-reel campaigns and reconciles comments that webhooks missed,
 * while BullMQ drains delayed jobs. This fork has no worker: an external
 * scheduler (cron-job.org, GitHub Actions, any cron) calls this route on that
 * same cadence and it does all three in one request, then records the heartbeat
 * the diagnostics page reads.
 *
 * Auth: `Authorization: Bearer <CRON_SECRET>`, or `?key=<CRON_SECRET>` for
 * schedulers that cannot set headers. Falls back to NEXTAUTH_SECRET, matching
 * the other cron routes in this app.
 */

import os from "node:os";
import { NextRequest, NextResponse } from "next/server";
import { attachPendingNextReels } from "@/lib/automation/attach-next-reel";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";
import { reconcileComments } from "@/lib/polling/comment-reconciler";
import { runDueDeferredJobs } from "@/lib/queue/inline";
import { prisma } from "@/lib/db/client";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
// Instagram calls plus deferred sends need more than the default budget.
export const maxDuration = 60;

function isAuthorized(request: NextRequest): boolean {
  const secret = process.env.CRON_SECRET || process.env.NEXTAUTH_SECRET;
  if (!secret) return false;

  const header = request.headers.get("authorization");
  if (header === `Bearer ${secret}`) return true;

  return request.nextUrl.searchParams.get("key") === secret;
}

export async function GET(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json(
      { success: false, error: "Unauthorized" },
      { status: 401 }
    );
  }

  const startedAt = Date.now();
  const errors: string[] = [];

  // Each stage is independent: a failure in one must not stop the others, or a
  // single bad Instagram account would block every campaign's DMs.
  let attached: Awaited<ReturnType<typeof attachPendingNextReels>> | null = null;
  try {
    attached = await attachPendingNextReels();
  } catch (error) {
    errors.push(
      `attach-next-reel: ${error instanceof Error ? error.message : "failed"}`
    );
  }

  let reconciled: Awaited<ReturnType<typeof reconcileComments>> | null = null;
  try {
    reconciled = await reconcileComments();
  } catch (error) {
    errors.push(
      `reconcile: ${error instanceof Error ? error.message : "failed"}`
    );
  }

  let deferred: Awaited<ReturnType<typeof runDueDeferredJobs>> | null = null;
  try {
    deferred = await runDueDeferredJobs();
  } catch (error) {
    errors.push(
      `deferred: ${error instanceof Error ? error.message : "failed"}`
    );
  }

  // The heartbeat is what the diagnostics page calls "worker health": it means
  // "a sweep ran recently", so it is written even when a stage failed.
  await recordWorkerHeartbeat({
    pid: process.pid,
    hostname: os.hostname(),
    startedAt: new Date(startedAt).toISOString(),
  }).catch(() => {});

  if (errors.length > 0) {
    await prisma.operationalEvent
      .create({
        data: {
          source: "SYSTEM",
          level: "WARNING",
          message: `Sweep completed with errors: ${errors.join("; ")}`,
          payload: { errors },
        },
      })
      .catch(() => {});
  }

  return NextResponse.json({
    success: errors.length === 0,
    durationMs: Date.now() - startedAt,
    attached,
    reconciled,
    deferred,
    errors,
  });
}

// Schedulers that only send POST get the same behaviour.
export async function POST(request: NextRequest) {
  return GET(request);
}
