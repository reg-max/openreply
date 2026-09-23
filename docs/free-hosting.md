# Running this fork on free tiers

Upstream OpenReply needs three always-on pieces: Postgres, Redis, and a Node
worker draining a BullMQ queue. The worker is what forces a paid host — free
plans either sleep idle processes or bill for the hours.

This fork removes Redis and the worker, so the whole stack fits on free plans:

| Piece | Upstream | Here |
| --- | --- | --- |
| Web app | Vercel | Vercel (unchanged) |
| Postgres | any | any, e.g. Neon or Supabase free plan |
| Redis | required | **not used** |
| Worker | always-on Node process | **not used** — an external cron calls `/api/cron/sweep` |

## What changed

- **`lib/queue/inline.ts`** replaces the BullMQ queue. Work with no delay runs
  inline, in the request that produced it (the Meta webhook, or the sweep). Work
  with a delay — rate-limit backoff, follow-up messages, the opening-DM read
  fallback — is stored in the `DeferredJob` table and run by the next sweep that
  finds it due. A job that throws is stored the same way and retried with a
  growing backoff, so a transient Instagram error does not lose a DM.
- **`lib/kv/pg-kv.ts`** replaces Redis as a TTL key-value store. The hourly
  private-reply counter, the sweep heartbeat, and the recent-alerts buffer live
  in the `KvEntry` table. The rate limiter still reserves slots atomically: the
  increment and the window's expiry are one statement.
- **`app/api/cron/sweep/route.ts`** is what the old worker loop did, as one HTTP
  request: attach pending next-reel campaigns, reconcile comments that webhooks
  missed, run due deferred jobs, write the heartbeat.
- Duplicate protection did not change: `processComment` and its siblings key on
  `DmLog` (`automationId` + `commentId`), so the same comment arriving twice —
  once by webhook, once by sweep — sends one DM.

## Setup

1. **Database**: create a free Postgres (Neon, Supabase, …) and set
   `DATABASE_URL` on Vercel. `prisma migrate deploy` runs during the Vercel
   build, so the new tables are created on the next deploy.
2. **Remove** `REDIS_URL` — nothing reads it any more.
3. **Set `CRON_SECRET`** on Vercel to a random string
   (`openssl rand -hex 16`).
4. **Schedule the sweep** every 5–15 minutes, whichever you prefer:
   - an external scheduler (cron-job.org and similar have free plans) calling
     `https://<your-app>/api/cron/sweep?key=<CRON_SECRET>`;
   - or the included GitHub Action (`.github/workflows/sweep.yml`), which needs
     the `APP_URL` and `CRON_SECRET` repository secrets. Note GitHub pauses
     scheduled workflows in a repository with no commits for 60 days.

The sweep also accepts `Authorization: Bearer <CRON_SECRET>`, and answers POST
as well as GET.

## What this costs you

- **Latency**: a webhook still sends its DM immediately, so a published Meta app
  replies in seconds. What slows down is the safety net — a comment a webhook
  missed waits for the next sweep instead of the worker's 5-minute loop.
- **Delayed features**: follow-ups and the opening-DM read fallback fire on the
  first sweep after they come due, so they are late by up to one sweep interval.
- **Throughput**: everything runs inside serverless functions, so a very large
  burst of comments is handled a request at a time rather than by a pool of
  five concurrent workers. Fine for one account; revisit before running this for
  many.
- **Upstream merges**: this fork touches the queue plumbing, so pulling upstream
  changes to `lib/queue/*` will need conflict resolution.

## Local development

`npm run worker` now runs the same sweep on a loop against your dev database,
so you do not need Redis locally either. `docker-compose` still starts Postgres;
the Redis service in it is unused.
