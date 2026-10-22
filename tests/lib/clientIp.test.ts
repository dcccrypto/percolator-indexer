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

  it("depth 2 peels two hops; a depth past the chain clamps to the leftmost", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "6.6.6.6, 203.0.113.7, 10.0.0.1" }), depth: 2 })).toBe("203.0.113.7");
    expect(clientIp({ header: h({ "x-forwarded-for": "203.0.113.7" }), depth: 3 })).toBe("203.0.113.7");
  });

  it("depth 0 trusts no forwarded header and uses the socket address", () => {
    expect(
      clientIp({ header: h({ "x-forwarded-for": "1.1.1.1", "x-real-ip": "2.2.2.2" }), remoteAddress: "198.51.100.4", depth: 0 }),
    ).toBe("198.51.100.4");
    expect(clientIp({ header: h({}), depth: 0 })).toBe(UNKNOWN_IP);
  });

  it("a non-IP hop falls back to x-real-ip, then the socket address", () => {
    expect(clientIp({ header: h({ "x-forwarded-for": "garbage", "x-real-ip": "203.0.113.9" }), depth: 1 })).toBe("203.0.113.9");
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
