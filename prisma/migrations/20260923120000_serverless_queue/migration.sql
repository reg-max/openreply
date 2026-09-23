-- Serverless replacements for Redis: TTL key-value store + delayed job store.

CREATE TABLE "KvEntry" (
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "expiresAt" TIMESTAMP(3),
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "KvEntry_pkey" PRIMARY KEY ("key")
);

CREATE INDEX "KvEntry_expiresAt_idx" ON "KvEntry"("expiresAt");

CREATE TABLE "DeferredJob" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "dedupeKey" TEXT,
    "data" JSONB NOT NULL,
    "runAt" TIMESTAMP(3) NOT NULL,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lockedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DeferredJob_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "DeferredJob_dedupeKey_key" ON "DeferredJob"("dedupeKey");
CREATE INDEX "DeferredJob_runAt_idx" ON "DeferredJob"("runAt");
