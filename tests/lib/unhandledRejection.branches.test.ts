import { describe, it, expect, vi } from "vitest";
import {
  describeRejection,
  createUnhandledRejectionHandler,
  installUnhandledRejectionPolicy,
} from "../../src/lib/unhandledRejection.js";

function throwingToString(): object {
  return {
    toString() {
      throw new Error("toString boom");
    },
  };
}

function throwingMessageError(): Error {
  const e = new Error("x");
  Object.defineProperty(e, "message", {
    get() {
      throw new Error("message getter boom");
    },
  });
  return e;
}

const cyclic: Record<string, unknown> = { name: "loop" };
cyclic.self = cyclic;

const shapes: Array<[string, () => unknown]> = [
  ["string", () => "plain failure"],
  ["empty string", () => ""],
  ["number", () => 42],
  ["NaN", () => NaN],
  ["bigint", () => 10n],
  ["symbol", () => Symbol("sym")],
  ["null", () => null],
  ["undefined", () => undefined],
  ["frozen object", () => Object.freeze({ a: 1 })],
  ["object with throwing toString", throwingToString],
  ["null-prototype object", () => Object.create(null)],
  ["Error whose message getter throws", throwingMessageError],
  ["cyclic object", () => cyclic],
  ["Error with cyclic cause", () => {
    const e = new Error("c", { cause: cyclic });
    return e;
  }],
  ["Error with non-string stack", () => {
    const e = new Error("s");
    (e as unknown as { stack: unknown }).stack = 123;
    return e;
  }],
  ["Error with no stack", () => {
    const e = new Error("s");
    delete (e as unknown as { stack?: string }).stack;
    return e;
  }],
  ["revoked proxy", () => {
    const r = Proxy.revocable({}, {});
    r.revoke();
    return r.proxy;
  }],
  ["throwing Symbol.toPrimitive", () => ({
    [Symbol.toPrimitive]() {
      throw new Error("prim boom");
    },
  })],
];

describe("describeRejection", () => {
  it.each(shapes)("never throws and returns plain strings for %s", (_n, make) => {
    const out = describeRejection(make());
    expect(typeof out.message).toBe("string");
    expect(out.stack === undefined || typeof out.stack === "string").toBe(true);
  });

  it("describes primitives with String()", () => {
    expect(describeRejection("boom").message).toBe("boom");
    expect(describeRejection(7).message).toBe("7");
    expect(describeRejection(null).message).toBe("null");
    expect(describeRejection(undefined).message).toBe("undefined");
  });

  it("keeps message and stack of a real Error", () => {
    const e = new Error("real");
    const out = describeRejection(e);
    expect(out.message).toBe("real");
    expect(out.stack).toBe(e.stack);
  });

  it("drops a non-string stack", () => {
    const e = new Error("s");
    (e as unknown as { stack: unknown }).stack = 123;
    expect(describeRejection(e)).toEqual({ message: "s", stack: undefined });
  });

  it("falls back to a fixed string when the reason cannot be printed", () => {
    expect(describeRejection(throwingToString()).message).toBe("unprintable rejection reason");
    expect(describeRejection(throwingMessageError()).message).toBe("unprintable rejection reason");
  });
});

describe("createUnhandledRejectionHandler", () => {
  it.each(shapes)("logs once and never throws for %s", (_n, make) => {
    const error = vi.fn();
    const handler = createUnhandledRejectionHandler({ error });
    expect(() => handler(make())).not.toThrow();
    expect(error).toHaveBeenCalledTimes(1);
    const [msg, meta] = error.mock.calls[0] as [string, { message: unknown; stack?: unknown }];
    expect(msg).toContain("process stays up");
    expect(typeof meta.message).toBe("string");
    expect(meta.stack === undefined || typeof meta.stack === "string").toBe(true);
    // Plain strings only: the meta must serialize even for cyclic reasons.
    expect(() => JSON.stringify(meta)).not.toThrow();
  });

  it.each(shapes)("does not throw when the logger itself throws (%s)", (_n, make) => {
    const handler = createUnhandledRejectionHandler({
      error: () => {
        throw new Error("logger down");
      },
    });
    expect(() => handler(make())).not.toThrow();
  });
});

describe("installUnhandledRejectionPolicy", () => {
  it("registers exactly the returned handler on unhandledRejection", () => {
    const on = vi.fn();
    const error = vi.fn();
    const handler = installUnhandledRejectionPolicy({ on }, { error });
    expect(on).toHaveBeenCalledTimes(1);
    expect(on).toHaveBeenCalledWith("unhandledRejection", handler);
    handler("x");
    expect(error).toHaveBeenCalledTimes(1);
  });
});
