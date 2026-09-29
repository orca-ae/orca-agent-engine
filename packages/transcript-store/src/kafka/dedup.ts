// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

export class LruDedup<V = string> {
  private readonly map = new Map<string, V>();
  constructor(private readonly capacity: number) {
    if (capacity <= 0) throw new Error('capacity must be > 0');
  }

  has(key: string): boolean {
    if (!this.map.has(key)) return false;
    const v = this.map.get(key)!;
    this.map.delete(key);
    this.map.set(key, v);
    return true;
  }

  get(key: string): V | undefined {
    if (!this.map.has(key)) return undefined;
    const v = this.map.get(key)!;
    this.map.delete(key);
    this.map.set(key, v);
    return v;
  }

  add(key: string, value: V): void {
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, value);
    while (this.map.size > this.capacity) {
      const oldest = this.map.keys().next().value;
      if (oldest === undefined) break;
      this.map.delete(oldest);
    }
  }
}
