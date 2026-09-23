/**
 * Postgres-backed key-value store with TTL semantics.
 *
 * This fork runs on serverless functions only (no Redis, no long-lived
 * worker), so the few places that used Redis as a TTL store — the hourly DM
 * rate-limit counter, the sweep heartbeat, the recent-alerts ring buffer — read
 * and write here instead. Expiry is enforced on read rather than by a reaper:
 * an entry whose `expiresAt` has passed is treated as absent and overwritten on
 * the next write.
 */

import { prisma } from "@/lib/db/client";

export async function kvGet(key: string): Promise<string | null> {
  const rows = await prisma.$queryRaw<{ value: string }[]>`
    SELECT "value" FROM "KvEntry"
    WHERE "key" = ${key}
      AND ("expiresAt" IS NULL OR "expiresAt" > NOW())
    LIMIT 1
  `;
  return rows[0]?.value ?? null;
}

export async function kvSet(
  key: string,
  value: string,
  ttlSeconds?: number
): Promise<void> {
  const expiresAt =
    ttlSeconds && ttlSeconds > 0
      ? new Date(Date.now() + ttlSeconds * 1000)
      : null;

  await prisma.kvEntry.upsert({
    where: { key },
    create: { key, value, expiresAt },
    update: { value, expiresAt },
  });
}

export async function kvDelete(key: string): Promise<void> {
  await prisma.kvEntry.deleteMany({ where: { key } });
}

/**
 * Atomic counter increment with a TTL applied on first write.
 *
 * A single statement so two concurrent function invocations cannot both read
 * the same count and each believe they own the last free slot. An expired row
 * restarts the window at 1 instead of continuing the old count.
 */
export async function kvIncrementWithTtl(
  key: string,
  ttlSeconds: number
): Promise<number> {
  const expiresAt = new Date(Date.now() + ttlSeconds * 1000);

  const rows = await prisma.$queryRaw<{ value: string }[]>`
    INSERT INTO "KvEntry" ("key", "value", "expiresAt", "updatedAt")
    VALUES (${key}, '1', ${expiresAt}, NOW())
    ON CONFLICT ("key") DO UPDATE SET
      "value" = CASE
        WHEN "KvEntry"."expiresAt" IS NOT NULL AND "KvEntry"."expiresAt" <= NOW()
          THEN '1'
        ELSE (COALESCE(NULLIF("KvEntry"."value", '')::bigint, 0) + 1)::text
      END,
      "expiresAt" = CASE
        WHEN "KvEntry"."expiresAt" IS NOT NULL AND "KvEntry"."expiresAt" <= NOW()
          THEN ${expiresAt}
        ELSE "KvEntry"."expiresAt"
      END,
      "updatedAt" = NOW()
    RETURNING "value"
  `;

  return Number.parseInt(rows[0]?.value ?? "0", 10);
}

/**
 * Give back a slot taken by kvIncrementWithTtl, preserving the window's expiry.
 * Clamped at zero so a rolled-over window cannot go negative.
 */
export async function kvDecrement(key: string): Promise<number> {
  const rows = await prisma.$queryRaw<{ value: string }[]>`
    UPDATE "KvEntry"
    SET "value" = GREATEST(COALESCE(NULLIF("value", '')::bigint, 0) - 1, 0)::text,
        "updatedAt" = NOW()
    WHERE "key" = ${key}
    RETURNING "value"
  `;
  return Number.parseInt(rows[0]?.value ?? "0", 10);
}

/** Read a JSON value, returning null when absent, expired or unparseable. */
export async function kvGetJson<T>(key: string): Promise<T | null> {
  const raw = await kvGet(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export async function kvSetJson(
  key: string,
  value: unknown,
  ttlSeconds?: number
): Promise<void> {
  await kvSet(key, JSON.stringify(value), ttlSeconds);
}

/**
 * Prepend to a capped list stored as a JSON array under one key. Replaces the
 * Redis LPUSH + LTRIM pair used for the recent-alerts buffer.
 */
export async function kvPushCapped<T>(
  key: string,
  entry: T,
  cap: number
): Promise<void> {
  const current = (await kvGetJson<T[]>(key)) ?? [];
  const next = [entry, ...current].slice(0, cap);
  await kvSetJson(key, next);
}

export async function kvReadList<T>(key: string, limit: number): Promise<T[]> {
  const current = (await kvGetJson<T[]>(key)) ?? [];
  return current.slice(0, Math.max(0, limit));
}
