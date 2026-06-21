/**
 * Token management: caching, expiry detection, and renewal.
 *
 * The SDK NEVER stores a token in source code. All tokens are obtained at
 * runtime via the application-supplied `getToken` callback and cached in memory.
 */

const EXPIRY_MARGIN_SECONDS = 30;

interface CachedToken {
  value: string;
  /** Unix epoch seconds */
  exp: number;
}

/**
 * Decode the `exp` claim from a JWT without verifying the signature.
 * The signature is verified server-side; the SDK only reads `exp` to know
 * when to refresh proactively.
 *
 * Returns 0 (treat as expired) if the JWT is malformed or lacks `exp`.
 */
function decodeExp(jwt: string): number {
  try {
    const parts = jwt.split(".");
    if (parts.length !== 3) return 0;
    // Base64URL → Base64 → JSON
    const payload = parts[1];
    if (!payload) return 0;
    const padded = payload.replace(/-/g, "+").replace(/_/g, "/");
    const decoded = atob(padded);
    const parsed: unknown = JSON.parse(decoded);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "exp" in parsed &&
      typeof (parsed as Record<string, unknown>)["exp"] === "number"
    ) {
      return (parsed as { exp: number }).exp;
    }
    return 0;
  } catch {
    return 0;
  }
}

function nowSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

function isExpiringSoon(exp: number): boolean {
  return nowSeconds() >= exp - EXPIRY_MARGIN_SECONDS;
}

export class TokenManager {
  private cached: CachedToken | null = null;
  private inflight: Promise<string> | null = null;

  constructor(private readonly getToken: () => Promise<string>) {}

  /**
   * Return a valid token, refreshing if expired or expiring within EXPIRY_MARGIN_SECONDS.
   * Concurrent callers share a single in-flight request.
   */
  async get(): Promise<string> {
    if (this.cached !== null && !isExpiringSoon(this.cached.exp)) {
      return this.cached.value;
    }
    return this.refresh();
  }

  /**
   * Force-refresh the token (called on 401 responses).
   * Concurrent force-refreshes are deduplicated.
   */
  async refresh(): Promise<string> {
    if (this.inflight !== null) {
      return this.inflight;
    }
    this.inflight = this.getToken().then((token) => {
      this.cached = { value: token, exp: decodeExp(token) };
      this.inflight = null;
      return token;
    }).catch((err: unknown) => {
      this.inflight = null;
      throw err;
    });
    return this.inflight;
  }

  /** Invalidate the cached token (e.g. after receiving a 401). */
  invalidate(): void {
    this.cached = null;
  }
}
