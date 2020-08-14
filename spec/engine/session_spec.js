const Session = require('../../engine/session.js');

const { SessionStateStore } = require('../../engine/session_state.js');
const { PlayheadStateStore } = require('../../engine/playhead_state.js');

describe("Session", () => {
  let sessionStore = undefined;
  beforeEach(() => {
    sessionStore = {
      sessionStateStore: new SessionStateStore(),
      playheadStateStore: new PlayheadStateStore()
    };  
  });

  it("creates a unique session ID", () => {
    const id1 = new Session('dummy', null, sessionStore).sessionId;
    const id2 = new Session('dummy', null, sessionStore).sessionId;
    expect(id1).not.toEqual(id2);
  });
});