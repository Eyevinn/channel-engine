const Session = require('../../engine/session.js');

describe("Session", () => {
  it("creates a unique session ID", () => {
    const id1 = new Session('dummy').sessionId;
    const id2 = new Session('dummy').sessionId;
    expect(id1).not.toEqual(id2);
  });
});