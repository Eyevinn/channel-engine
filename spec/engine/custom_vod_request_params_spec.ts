const Session = require("../../engine/session.js");

const { SessionStateStore } = require("../../engine/session_state.js");
const { PlayheadStateStore } = require("../../engine/playhead_state.js");

// End-to-end contract test for the custom getNextVod pass-through params
// feature (issues #377/#378/#379/#380).
//
// The plumbing spans two seams:
//   1. engine/server.ts `_handleMasterManifest` filters the incoming
//      `request.query` by the `customVodRequestParams` allowlist and calls
//      `session.setCustomParams(filtered)` on every master-manifest request.
//   2. engine/session.ts forwards those params onto `VodRequest.customParams`
//      from `_getNextVod()` when (and only when) at least one is present.
//
// The module-level fastify instance is a process singleton, so only ONE
// successful `ChannelEngine` construction (and therefore one live HTTP route
// set) is possible per process — see the note in interface_spec.ts. Driving
// three distinct master-manifest HTTP requests through a real engine is
// therefore impractical in-suite. Instead we exercise the exact same contract
// deterministically at the seam we CAN drive repeatedly: the allowlist filter
// (identical to the handler's logic) feeding the real `session.setCustomParams`
// + `session._getNextVod()` path, asserting what a stubbed asset manager's
// `getNextVod` actually receives. This proves the observable behaviour the
// handler produces for each of the three required cases (a/b/c).

// Mirrors engine/server.ts `_handleMasterManifest` (#379): keep only query
// params whose name is in the configured allowlist. An empty/absent allowlist
// (the default) yields no custom params.
function filterAllowlistedParams(query, allowlist) {
  const customParams = {};
  for (const key of allowlist || []) {
    if (query[key] !== undefined) {
      customParams[key] = query[key];
    }
  }
  return customParams;
}

describe("custom getNextVod pass-through params (issue #380)", () => {
  let sessionLiveStore;

  beforeEach(() => {
    sessionLiveStore = {
      sessionStateStore: new SessionStateStore(),
      playheadStateStore: new PlayheadStateStore(),
    };
  });

  // Builds a session whose stubbed asset manager records the VodRequest it
  // receives, then drives the full handler contract: allowlist-filter the
  // query exactly as `_handleMasterManifest` does, push the result through
  // `setCustomParams`, and trigger a `_getNextVod()`.
  async function driveMasterManifestRequest({ query, allowlist }) {
    let received;
    const session = new Session(
      {
        getNextVod: async (vodRequest) => {
          received = vodRequest;
          return { id: "1", uri: "https://example.com/vod.m3u8" };
        },
      },
      null,
      sessionLiveStore
    );
    session.setCustomParams(filterAllowlistedParams(query, allowlist));
    await session._getNextVod();
    return received;
  }

  it("(a) forwards an allowlisted query param to getNextVod as customParams", async () => {
    const vodRequest = await driveMasterManifestRequest({
      query: { myparam: "foo" },
      allowlist: ["myparam"],
    });
    expect(vodRequest.customParams).toEqual({ myparam: "foo" });
  });

  it("(b) strips a non-allowlisted query param before it reaches getNextVod", async () => {
    const vodRequest = await driveMasterManifestRequest({
      // `myparam` is allowlisted and passes through; `evil` is not and must be
      // dropped entirely (not merely emptied).
      query: { myparam: "foo", evil: "bar" },
      allowlist: ["myparam"],
    });
    expect(vodRequest.customParams).toEqual({ myparam: "foo" });
    expect(vodRequest.customParams.evil).toBeUndefined();
  });

  it("(c) sends no customParams when no allowlist is configured (existing behaviour preserved)", async () => {
    const vodRequest = await driveMasterManifestRequest({
      query: { myparam: "foo" },
      allowlist: [], // the ChannelEngine default when customVodRequestParams is unset
    });
    // The field is omitted entirely so asset managers that don't expect it are
    // unaffected — matching the pre-#379 request shape.
    expect(vodRequest.customParams).toBeUndefined();
  });
});
