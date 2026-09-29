// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/** Session JWT expiry is Unix seconds; Date.now() returns milliseconds. */
export interface SessionJwt {
  readonly token: string;
  readonly expiresAt: number;
}

const REFRESH_AHEAD_MS = 30_000;
const REFRESH_TIMEOUT_MS = 10_000;

/** Demand-driven, single-flight refresh. Caller AbortError never cancels the shared mint. */
export class SessionJwtProvider {
  private cache: SessionJwt | undefined;
  private refreshAt = 0;
  private flight: Promise<SessionJwt> | undefined;
  private controller: AbortController | undefined;
  private closed = false;

  constructor(
    private readonly mint: (signal: AbortSignal) => Promise<SessionJwt>,
    private readonly minimumValidityMs = 0,
  ) {}

  async getValidToken(signal?: AbortSignal): Promise<SessionJwt> {
    if (this.closed) throw new Error('Session JWT provider closed');
    if (signal?.aborted) throw new DOMException('Session JWT request cancelled', 'AbortError');
    const now = Date.now();
    if (
      this.cache &&
      now < this.refreshAt &&
      now + this.minimumValidityMs < this.cache.expiresAt * 1000
    ) {
      return this.cache;
    }
    const flight = this.flight ?? this.refresh();
    if (!signal) return flight;
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        signal.removeEventListener('abort', onAbort);
        reject(new DOMException('Session JWT request cancelled', 'AbortError'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
      if (signal.aborted) onAbort();
      // Both handlers consume the shared outcome even if this caller has cancelled.
      flight.then(
        (jwt) => {
          signal.removeEventListener('abort', onAbort);
          resolve(jwt);
        },
        (error: unknown) => {
          signal.removeEventListener('abort', onAbort);
          reject(error);
        },
      );
    });
  }

  close(): void {
    this.closed = true;
    this.cache = undefined;
    this.controller?.abort(new Error('Session JWT provider closed'));
  }

  private refresh(): Promise<SessionJwt> {
    const controller = new AbortController();
    this.controller = controller;
    let onAbort!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(controller.signal.reason);
      controller.signal.addEventListener('abort', onAbort, { once: true });
    });
    const timer = setTimeout(() => {
      controller.abort(new Error('Session JWT refresh timed out'));
    }, REFRESH_TIMEOUT_MS);
    const minted = (async () => {
      try {
        return await this.mint(controller.signal);
      } catch {
        throw new Error('Session JWT refresh failed');
      }
    })();
    // Race the promise, not just the signal: an uncooperative minter is still bounded.
    // Promise.race also consumes any rejection arriving after timeout/close.
    this.flight = Promise.race([minted, aborted])
      .then((jwt) => {
        if (this.closed) throw new Error('Session JWT provider closed');
        if (controller.signal.aborted) throw controller.signal.reason;
        const now = Date.now();
        if (!isValidJwt(jwt, now)) throw new Error('Session JWT refresh failed');
        if (now + this.minimumValidityMs >= jwt.expiresAt * 1000)
          throw new Error('Session JWT lifetime is shorter than the required execution window');
        return this.install(jwt, now);
      })
      .finally(() => {
        clearTimeout(timer);
        controller.signal.removeEventListener('abort', onAbort);
        this.controller = undefined;
        this.flight = undefined;
      });
    return this.flight;
  }

  private install(jwt: SessionJwt, now: number): SessionJwt {
    this.cache = Object.freeze({ token: jwt.token, expiresAt: jwt.expiresAt });
    const remainingMs = jwt.expiresAt * 1000 - now;
    this.refreshAt = jwt.expiresAt * 1000 - Math.min(REFRESH_AHEAD_MS, remainingMs / 5);
    return this.cache;
  }
}

function isValidJwt(value: unknown, now: number): value is SessionJwt {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const jwt = value as Partial<SessionJwt>;
  return (
    typeof jwt.token === 'string' &&
    jwt.token.length > 0 &&
    !/\s/.test(jwt.token) &&
    typeof jwt.expiresAt === 'number' &&
    Number.isSafeInteger(jwt.expiresAt) &&
    jwt.expiresAt > now / 1000
  );
}
