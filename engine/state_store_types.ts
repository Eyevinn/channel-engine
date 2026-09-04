// Shared type definitions for the engine state-store layer (#373).
// Type-only declarations — no runtime code is emitted for this module beyond
// an empty object, and nothing here changes runtime behavior.

/**
 * The low-level key/value store contract implemented by the memory, memcached
 * and redis backends and driven by `SharedStateStore`.
 */
export interface IStateStore {
  initAsync(id: string, initData: Record<string, any>): Promise<any>;
  resetAsync(id: string, initData: Record<string, any>): Promise<void>;
  resetAllAsync(): Promise<void>;
  getAsync(id: string, key: string): Promise<any>;
  setAsync(id: string, key: string, value: any): Promise<any>;
  setVolatileAsync(id: string, key: string, value: any): Promise<any>;
  removeAsync(id: string, key: string): Promise<any>;
  getValues?(id: string, keys: string[]): Promise<Record<string, any>>;
  setValues?(id: string, data: Record<string, any>): Promise<Record<string, any>>;
}

export type SharedStates = { [id: string]: { [key: string]: any } | null };

export interface ISharedStateStoreOpts {
  cacheTTL?: number;
  redisUrl?: string;
  memcachedUrl?: string;
  version?: string;
  volatileKeyTTL?: number;
  [key: string]: any;
}

export interface ILeaderCache {
  ts: number;
  value: string | null;
}
