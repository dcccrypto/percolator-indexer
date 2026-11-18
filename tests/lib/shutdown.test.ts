import { describe, it, expect, vi, afterEach } from "vitest";
import { parseDecimalIntEnv, runShutdown } from "../../src/lib/shutdown.js";

afterEach(() => vi.useRealTimers());

describe("parseDecimalIntEnv", () => {
  it.each([
    [undefined, 30000],
    ["", 30000],
    [" ", 30000],
    ["abc", 30000],
    ["0", 30000],
    ["1e3", 30000],
    ["0x10", 30000],
    ["-5", 30000],
    ["1.5", 30000],
    ["99999999999999999999", 30000],
    ["1500", 1500],
    [" 2000 ", 2000],
  ])("%j -> %d", (raw, want) => {
    expect(parseDecimalIntEnv(raw, 30000)).toBe(want);
  });
});

describe("parseDecimalIntEnv min", () => {
  it("allows 0 when min is 0", () => {
    expect(parseDecimalIntEnv("0", 5000, 0)).toBe(0);
    expect(parseDecimalIntEnv("1e3", 5000, 0)).toBe(5000);
  });
});

describe("runShutdown", () => {
  function deps(over: Partial<Parameters<typeof runShutdown>[0]> = {}) {
    const order: string[] = [];
    return {
      order,
      d: {
        stopSteps: [() => order.push("http"), () => order.push("stream"), () => order.push("services")],
        alert: async () => {
          order.push("alert");
        },
        alertTimeoutMs: 2000,
        drainMs: 3000,
        onAlertError: () => order.push("alert-error"),
        onStepError: () => order.push("step-error"),
        exit: (c: number) => order.push(`exit${c}`),
        ...over,
      },
    };
  }

  it("stops services before sending the alert, then exits 0", async () => {
    vi.useFakeTimers();
    const { order, d } = deps();
    const p = runShutdown(d);
    await vi.advanceTimersByTimeAsync(3000);
    await p;
    expect(order).toEqual(["http", "stream", "services", "alert", "exit0"]);
  });

  it("a hung alert cannot block shutdown past its timeout", async () => {
    vi.useFakeTimers();
    const { order, d } = deps({ alert: () => new Promise(() => {}) });
    const p = runShutdown(d);
    await vi.advanceTimersByTimeAsync(2000 + 3000);
    await p;
    expect(order).toEqual(["http", "stream", "services", "alert-error", "exit0"]);
  });

  it("a throwing alert still stops services and exits 0", async () => {
    vi.useFakeTimers();
    const { order, d } = deps({ alert: async () => { throw new Error("webhook 500"); } });
    const p = runShutdown(d);
    await vi.advanceTimersByTimeAsync(3000);
    await p;
    expect(order).toEqual(["http", "stream", "services", "alert-error", "exit0"]);
  });

  it("a throwing stop step does not skip later steps", async () => {
    vi.useFakeTimers();
    const { order, d } = deps();
    d.stopSteps[0] = () => { throw new Error("close failed"); };
    const p = runShutdown(d);
    await vi.advanceTimersByTimeAsync(3000);
    await p;
    expect(order).toEqual(["step-error", "stream", "services", "alert", "exit0"]);
  });
});
