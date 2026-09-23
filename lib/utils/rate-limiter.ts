/**
 * Rate Limiter
 *
 * Postgres-backed rate limiter for Instagram private replies (this fork runs
 * without Redis; see lib/kv/pg-kv.ts).
 *
 * The cap matches Meta's documented limit for this exact call: 750 private
 * replies per hour per Instagram professional account, for comments on posts
 * and reels. Exceeding it risks 429s and app-level restrictions, so the sender
 * defers rather than pushing through.
 * https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
 *
 * Note this is a hard ceiling with no headroom. If Meta throttles before the
 * documented limit, or other calls on the same account share the bucket, lower
 * this value.
 */

import {
  kvDecrement,
  kvDelete,
  kvGet,
  kvIncrementWithTtl,
} from "@/lib/kv/pg-kv";

const RATE_LIMIT_MAX = 750; // private replies per hour, per Meta's documented cap
const RATE_LIMIT_WINDOW = 3600; // 1 hour in seconds
const REQUEUE_DELAY_MS = 30 * 60 * 1000; // 30 minutes
const MAX_REQUEUE_ATTEMPTS = 3;

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  remainingDMs: number;
  shouldRequeue: boolean;
  requeueDelayMs: number;
  shouldSkip: boolean;
  reserved: boolean;
}

function counterKey(instagramAccountId: string): string {
  return `rate:dm:${instagramAccountId}`;
}

function blockedResult(count: number, requeueAttempt: number): RateLimitResult {
  if (requeueAttempt >= MAX_REQUEUE_ATTEMPTS) {
    return {
      allowed: false,
      currentCount: count,
      remainingDMs: 0,
      shouldRequeue: false,
      requeueDelayMs: 0,
      shouldSkip: true,
      reserved: false,
    };
  }

  return {
    allowed: false,
    currentCount: count,
    remainingDMs: 0,
    shouldRequeue: true,
    requeueDelayMs: REQUEUE_DELAY_MS,
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Check if an Instagram account is within its DM rate limit.
 *
 * Read-only: it reports the current window without taking a slot. Callers that
 * are about to send must use reserveDMSlot instead.
 */
export async function checkRateLimit(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const raw = await kvGet(counterKey(instagramAccountId));
  const count = raw ? Number.parseInt(raw, 10) : 0;

  if (count >= RATE_LIMIT_MAX) {
    return blockedResult(count, requeueAttempt);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: RATE_LIMIT_MAX - count,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Atomically reserve a DM send slot for an Instagram account.
 *
 * The increment and the window's TTL are applied in one statement, so two
 * concurrent invocations cannot both pass the check on the last free slot. When
 * the increment lands above the cap the slot is handed straight back, so a
 * blocked attempt does not inflate the counter.
 */
export async function reserveDMSlot(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const key = counterKey(instagramAccountId);
  const count = await kvIncrementWithTtl(key, RATE_LIMIT_WINDOW);

  if (count > RATE_LIMIT_MAX) {
    const restored = await kvDecrement(key);
    return blockedResult(restored, requeueAttempt);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: RATE_LIMIT_MAX - count,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: true,
  };
}

/**
 * Release a DM slot previously taken by reserveDMSlot.
 *
 * reserveDMSlot increments the hourly counter before the send, so concurrent
 * sends can't all pass the check at once. When that send then fails (closed
 * messaging window, expired token, rejected reply) the reserved slot is never
 * used and must be handed back. Otherwise a comment that never delivers a DM
 * still burns one slot per attempt, and retries burn several. On a post with
 * many failing sends the counter inflates past the real number of DMs and
 * legitimate replies get skipped as rate-limited until the window expires.
 *
 * The decrement preserves the key's expiry, so the hourly window still resets
 * when it originally would, and is clamped at zero.
 */
export async function releaseDMSlot(
  instagramAccountId: string
): Promise<number> {
  return kvDecrement(counterKey(instagramAccountId));
}

/**
 * Backwards-compatible helper for tests and admin scripts.
 * Prefer reserveDMSlot in senders.
 */
export async function incrementDMCounter(
  instagramAccountId: string
): Promise<number> {
  const result = await reserveDMSlot(instagramAccountId, MAX_REQUEUE_ATTEMPTS);
  return result.currentCount;
}

/**
 * Get the current DM count for an Instagram account.
 */
export async function getCurrentDMCount(
  instagramAccountId: string
): Promise<number> {
  const raw = await kvGet(counterKey(instagramAccountId));
  return raw ? Number.parseInt(raw, 10) : 0;
}

/**
 * Reset the rate limiter for an account (useful for testing).
 */
export async function resetRateLimit(
  instagramAccountId: string
): Promise<void> {
  await kvDelete(counterKey(instagramAccountId));
}

// Export constants for use in tests
export { RATE_LIMIT_MAX, RATE_LIMIT_WINDOW, REQUEUE_DELAY_MS, MAX_REQUEUE_ATTEMPTS };
