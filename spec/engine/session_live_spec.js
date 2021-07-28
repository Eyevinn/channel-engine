const SessionLive = require('../../engine/session_live.js');
const { SessionLiveStateStore } = require('../../engine/session_live_state.js');

describe("SessionLive", () => {
  let sessionLiveStore = undefined;
  beforeEach(() => {
    sessionLiveStore = {
      sessionStateStore: new SessionLiveStateStore()
    };
  });

  it("creates a unique sessionLive ID", () => {
    const id1 = new SessionLive(null, sessionLiveStore).sessionId;
    const id2 = new SessionLive(null, sessionLiveStore).sessionId;
    expect(id1).not.toEqual(id2);
  });
});