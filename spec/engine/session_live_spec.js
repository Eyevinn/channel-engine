const SessionLive = require('../../engine/session_live.js');

describe("SessionLive", () => {
  it("creates a unique sessionLive ID", () => {
    const id1 = new SessionLive(null).sessionId;
    const id2 = new SessionLive(null).sessionId;
    expect(id1).not.toEqual(id2);
  });
});