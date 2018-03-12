class EventStream {
  constructor(session) {
    this._session = session;
  }

  poll() {
    return new Promise((resolve, reject) => {
      let event = {};
      event = this._session.consumeEvent();
      if (event) {
        resolve(JSON.stringify(event));
      } else {
        resolve(JSON.stringify({}));        
      }
    });
  }
}

module.exports = EventStream;