/**
 * Insertion-ordered map with a size cap and a TTL. When full, the OLDEST entry is evicted (never
 * `clear()`: one burst must not forget every pending entry). Expired entries read as absent.
 */
export class BoundedTtlMap<K, V> {
  private m = new Map<K, { v: V; at: number }>();
  constructor(private readonly max: number, private readonly ttlMs: number, private readonly now: () => number = Date.now) {}

  get(k: K): V | undefined {
    const e = this.m.get(k);
    if (!e) return undefined;
    if (this.ttlMs === Infinity) return e.v; // never-expiring maps (lotScale) ignore the clock entirely
    const age = this.now() - e.at;
    // A negative (backwards wall-clock step) or non-finite age is expired, not "young".
    if (!(age >= 0 && age <= this.ttlMs)) { this.m.delete(k); return undefined; }
    return e.v;
  }
  set(k: K, v: V): void {
    this.m.delete(k); // re-insert = newest
    this.m.set(k, { v, at: this.now() });
    while (this.m.size > this.max) {
      const oldest = this.m.keys().next();
      if (oldest.done) break;
      this.m.delete(oldest.value);
    }
  }
  delete(k: K): void { this.m.delete(k); }
  get size(): number { return this.m.size; }
}
