/**
 * Token sub-module: caching, expiry detection, and renewal of the ingestion JWT.
 *
 * The SDK NEVER stores a token in source code. All tokens are obtained at
 * runtime via the application-supplied `getToken` callback and cached in memory.
 */

export { TokenManager } from "./manager.js";
