// TS migration bridge (see CONTRIBUTING.md "TypeScript migration").
//
// The engine tree is being migrated incrementally to TypeScript. Consumers
// (other engine modules and the specs) still `require('./foo.js')` with a
// literal `.js` extension, but the target file may already have been converted
// to `foo.ts`.
//
// This jasmine helper (helpers load before specs) installs two shims used only
// during the source-tree test run:
//
//   1. A `.js` -> `.ts` filename-resolution fallback, so existing
//      `require('./foo.js')` calls keep working after `foo.js` becomes `foo.ts`.
//   2. A `require.extensions['.ts']` compile hook that transpiles the `.ts`
//      source to CommonJS with the `typescript` package and hands the emitted
//      JS to `module._compile`.
//
// Crucially, ALL `.ts` loading goes through `ts.transpileModule` rather than
// Node's built-in type-stripping, so the tests behave identically on Node 20.x
// (no strip-types) and Node 22.x (strip-types varies by minor). Production is
// unaffected — it runs the compiled `dist/` tree, not this hook.
const fs = require("fs");
const Module = require("module");
const ts = require("typescript");

// ---------------------------------------------------------------------------
// 1. `.js` -> `.ts` resolution fallback.
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// 2. `.ts` compile hook — transpile with `typescript`, never native strip-types.
//    `require.extensions['.js']` is deliberately left untouched so the many
//    remaining `.js` engine modules keep loading the normal way.
// ---------------------------------------------------------------------------
require.extensions[".ts"] = function (module, filename) {
  const source = fs.readFileSync(filename, "utf8");
  const { outputText } = ts.transpileModule(source, {
    fileName: filename,
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
      esModuleInterop: true
    }
  });
  module._compile(outputText, filename);
};
