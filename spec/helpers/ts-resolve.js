// TS migration bridge (see CONTRIBUTING.md "TypeScript migration").
//
// The engine tree is being migrated incrementally to TypeScript. Consumers
// (other engine modules and the specs) still `require('./foo.js')` with a
// literal `.js` extension, but the target file may already have been converted
// to `foo.ts`. Node 22's built-in type-stripping can load a `.ts` module at
// runtime, but it will not fall back from a requested `.js` path to a `.ts`
// file on disk.
//
// This jasmine helper installs a resolver shim: when a `.js` request cannot be
// resolved, it retries the same path with a `.ts` extension. This lets the
// still-`.js` callers keep their existing `require('./foo.js')` calls unchanged
// while the underlying module is TypeScript. It is a build/test-time shim only
// and does not alter any engine runtime behavior.
const Module = require("module");

const originalResolveFilename = Module._resolveFilename;
Module._resolveFilename = function (request, parent, isMain, options) {
  try {
    return originalResolveFilename.call(this, request, parent, isMain, options);
  } catch (err) {
    if (typeof request === "string") {
      // `require('./foo.js')` -> try `./foo.ts`
      // `require('./foo')`    -> try `./foo.ts`
      const candidate = request.endsWith(".js")
        ? request.slice(0, -3) + ".ts"
        : request + ".ts";
      try {
        return originalResolveFilename.call(this, candidate, parent, isMain, options);
      } catch (_) {
        throw err;
      }
    }
    throw err;
  }
};
