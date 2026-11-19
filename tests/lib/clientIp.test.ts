import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { clientIp, trustedProxyDepth, UNKNOWN_IP } from "../../src/lib/clientIp.js";

const h = (headers: Record<string, string>) => (n: string) => headers[n.toLowerCase()];

describe("S3-C11: the webhook rate limiter does not trust a caller-written X-Forwarded-For", () => {
  it("index.ts no longer keys the limiter on the leftmost X-Forwarded-For entry", () => {
    const src = readFileSync(join(__dirname, "..", "..", "src", "index.ts"), "utf8");
    expect(src).not.toMatch(/x-forwarded-for"\)\?\.split\(","\)\[0\]/);
    expect(src).toMatch(/clientIp\(\{/);
  });

  it("depth 1 (default): a spoofed left entry is ignored; the proxy-appended rightmost hop is the key", () => {
    const depth = trustedProxyDepth(undefined);
    expect(depth).toBe(1);
    const a = clientIp({ header: h({ "x-forwarded-for": "1.1.1.1, 203.0.113.7" }), depth });
    const b = clientIp({ header: h({ "x-forwarded-for": "9.9.9.9, 203.0.113.7" }), depth });
    expect(a).toBe("203.0.113.7");
    expect(b).toBe(a);
  });

  it("depth 2 peels two hops", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "6.6.6.6, 203.0.113.7, 10.0.0.1" }), depth: 2 })).toBe("203.0.113.7");
  });

  it("a chain shorter than the depth is not trusted: a rotated caller-written entry yields ONE key, not many", () => {
    const keys = new Set(
      ["1.1.1.1", "2.2.2.2", "3.3.3.3"].map((ip) =>
        clientIp({ header: h({ "x-forwarded-for": ip }), remoteAddress: "198.51.100.4", depth: 2 }),
      ),
    );
    expect([...keys]).toEqual(["198.51.100.4"]);
    expect(clientIp({ header: h({ "x-forwarded-for": "203.0.113.7" }), depth: 3 })).toBe(UNKNOWN_IP);
  });

  it("a chain exactly as long as the depth is trusted (boundary), and a valid XFF beats a rotating X-Real-IP", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "203.0.113.7" }), remoteAddress: "10.0.0.1", depth: 1 })).toBe("203.0.113.7");
    expect(clientIp({ header: h({ "x-forwarded-for": "6.6.6.6, 203.0.113.7" }), remoteAddress: "10.0.0.1", depth: 2 })).toBe("6.6.6.6");
    expect(
      clientIp({ header: h({ "x-forwarded-for": "203.0.113.7", "x-real-ip": "9.9.9.9" }), remoteAddress: "10.0.0.1", depth: 1 }),
    ).toBe("203.0.113.7");
  });

  it("X-Real-IP is caller-writable and never keys the limiter: rotating it yields ONE key", () => {
    const keys = new Set(
      ["1.1.1.1", "2.2.2.2", "3.3.3.3"].map((ip) =>
        clientIp({ header: h({ "x-real-ip": ip }), remoteAddress: "198.51.100.4", depth: 1 }),
      ),
    );
    expect([...keys]).toEqual(["198.51.100.4"]);
  });

  it("depth 0 trusts no forwarded header and uses the socket address", () => {
    expect(
      clientIp({ header: h({ "x-forwarded-for": "1.1.1.1", "x-real-ip": "2.2.2.2" }), remoteAddress: "198.51.100.4", depth: 0 }),
    ).toBe("198.51.100.4");
    expect(clientIp({ header: h({}), depth: 0 })).toBe(UNKNOWN_IP);
  });

  it("a non-IP hop falls back to the socket address, ignoring x-real-ip", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "garbage", "x-real-ip": "203.0.113.9" }), depth: 1 })).toBe(UNKNOWN_IP);
    expect(clientIp({ header: h({ "x-forwarded-for": "garbage" }), remoteAddress: "::1", depth: 1 })).toBe("::1");
  });

  it("TRUSTED_PROXY_DEPTH parsing: integers 0..8 accepted, everything else is 1 and logged once", () => {
    const log = vi.fn();
    expect(trustedProxyDepth(" 2 ", log)).toBe(2);
    expect(trustedProxyDepth("0", log)).toBe(0);
    expect(trustedProxyDepth("", log)).toBe(1);
    for (const bad of ["abc", "-1", "1.5", "9"]) expect(trustedProxyDepth(bad, log)).toBe(1);
    expect(trustedProxyDepth("abc", log)).toBe(1);
    expect(log).toHaveBeenCalledTimes(4);
  });
});

describe("clientIp / trustedProxyDepth: remaining edges", () => {
  it("an invalid depth logs through the default console.error sink (once per distinct value)", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(trustedProxyDepth("not-a-depth-xyz")).toBe(1);
      expect(trustedProxyDepth("not-a-depth-xyz")).toBe(1);
      expect(spy).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
    }
  });

  it("reads TRUSTED_PROXY_DEPTH from the environment by default", () => {
    const prev = process.env.TRUSTED_PROXY_DEPTH;
    process.env.TRUSTED_PROXY_DEPTH = " 3 ";
    try {
      expect(trustedProxyDepth()).toBe(3);
    } finally {
      if (prev === undefined) delete process.env.TRUSTED_PROXY_DEPTH;
      else process.env.TRUSTED_PROXY_DEPTH = prev;
    }
  });

  it("an empty or whitespace/comma-only X-Forwarded-For falls back to the socket address", () => {
    for (const xff of ["", " , ,"]) {
      expect(clientIp({ header: h({ "x-forwarded-for": xff }), remoteAddress: "198.51.100.4", depth: 1 })).toBe("198.51.100.4");
    }
  });

  it("IPv6 hops are accepted; a non-IP chosen hop or bad socket address is not", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "1.1.1.1, 2001:db8::1" }), depth: 1 })).toBe("2001:db8::1");
    expect(clientIp({ header: h({ "x-forwarded-for": "1.1.1.1, evil" }), remoteAddress: "::1", depth: 1 })).toBe("::1");
    expect(clientIp({ header: h({ "x-forwarded-for": "1.1.1.1, evil" }), remoteAddress: "garbage", depth: 1 })).toBe(UNKNOWN_IP);
  });
});
