/**
 * Rate Limiter — Unit Tests
 *
 * Tests the hourly private-reply cap enforcement against a mocked Postgres
 * key-value store (this fork runs without Redis; see lib/kv/pg-kv.ts).
 * Assertions derive from RATE_LIMIT_MAX so they survive a change to the cap.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockGet, mockIncrement, mockDecrement, mockDelete } = vi.hoisted(
  () => ({
    mockGet: vi.fn(),
    mockIncrement: vi.fn(),
    mockDecrement: vi.fn(),
    mockDelete: vi.fn(),
  })
);

vi.mock("@/lib/kv/pg-kv", () => ({
  kvGet: mockGet,
  kvIncrementWithTtl: mockIncrement,
  kvDecrement: mockDecrement,
  kvDelete: mockDelete,
}));

import {
  checkRateLimit,
  incrementDMCounter,
  reserveDMSlot,
  releaseDMSlot,
  resetRateLimit,
  RATE_LIMIT_MAX,
  RATE_LIMIT_WINDOW,
} from "../lib/utils/rate-limiter";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("checkRateLimit", () => {
  it("should allow when count is below limit", async () => {
    mockGet.mockResolvedValue("50");

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(true);
    expect(result.currentCount).toBe(50);
    expect(result.remainingDMs).toBe(RATE_LIMIT_MAX - 50);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(false);
    expect(result.reserved).toBe(false);
  });

  it("should allow when no previous count exists", async () => {
    mockGet.mockResolvedValue(null);

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(true);
    expect(result.currentCount).toBe(0);
    expect(result.remainingDMs).toBe(RATE_LIMIT_MAX);
  });

  it("should not take a slot", async () => {
    mockGet.mockResolvedValue("50");

    await checkRateLimit("account_123");

    expect(mockIncrement).not.toHaveBeenCalled();
  });

  it("should deny when count reaches the limit", async () => {
    mockGet.mockResolvedValue(String(RATE_LIMIT_MAX));

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(true);
    expect(result.shouldSkip).toBe(false);
  });

  it("should skip after max requeue attempts", async () => {
    mockGet.mockResolvedValue(String(RATE_LIMIT_MAX));

    const result = await checkRateLimit("account_123", 3);

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(true);
  });
});

describe("reserveDMSlot", () => {
  it("should atomically reserve a slot when below the hourly cap", async () => {
    mockIncrement.mockResolvedValue(51);

    const result = await reserveDMSlot("account_123");

    expect(mockIncrement).toHaveBeenCalledWith(
      "rate:dm:account_123",
      RATE_LIMIT_WINDOW
    );
    expect(result.allowed).toBe(true);
    expect(result.reserved).toBe(true);
    expect(result.currentCount).toBe(51);
    expect(result.remainingDMs).toBe(RATE_LIMIT_MAX - 51);
  });

  it("should recommend requeue when the reservation lands over the cap", async () => {
    mockIncrement.mockResolvedValue(RATE_LIMIT_MAX + 1);
    mockDecrement.mockResolvedValue(RATE_LIMIT_MAX);

    const result = await reserveDMSlot("account_123", 0);

    expect(result.allowed).toBe(false);
    expect(result.reserved).toBe(false);
    expect(result.shouldRequeue).toBe(true);
    expect(result.shouldSkip).toBe(false);
  });

  it("should hand the slot back when the reservation is denied", async () => {
    mockIncrement.mockResolvedValue(RATE_LIMIT_MAX + 1);
    mockDecrement.mockResolvedValue(RATE_LIMIT_MAX);

    const result = await reserveDMSlot("account_123", 0);

    // A blocked attempt must not leave the counter inflated, or the window
    // would drift further past the cap on every retry.
    expect(mockDecrement).toHaveBeenCalledWith("rate:dm:account_123");
    expect(result.currentCount).toBe(RATE_LIMIT_MAX);
  });

  it("should skip after max requeue attempts", async () => {
    mockIncrement.mockResolvedValue(RATE_LIMIT_MAX + 1);
    mockDecrement.mockResolvedValue(RATE_LIMIT_MAX);

    const result = await reserveDMSlot("account_123", 3);

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(true);
  });
});

describe("incrementDMCounter", () => {
  it("should use the atomic reservation path", async () => {
    mockIncrement.mockResolvedValue(51);

    const count = await incrementDMCounter("account_123");

    expect(mockIncrement).toHaveBeenCalled();
    expect(count).toBe(51);
  });
});

describe("releaseDMSlot", () => {
  it("hands a reserved slot back and returns the new count", async () => {
    mockDecrement.mockResolvedValue(49);

    const count = await releaseDMSlot("account_123");

    expect(mockDecrement).toHaveBeenCalledWith("rate:dm:account_123");
    expect(count).toBe(49);
  });

  it("reports zero when the window already rolled over", async () => {
    mockDecrement.mockResolvedValue(0);

    const count = await releaseDMSlot("account_123");

    expect(count).toBe(0);
  });
});

describe("resetRateLimit", () => {
  it("clears the account's counter", async () => {
    await resetRateLimit("account_123");

    expect(mockDelete).toHaveBeenCalledWith("rate:dm:account_123");
  });
});
