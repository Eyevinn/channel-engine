class EventStream {
  private _session: any;

  constructor(session: any) {
    this._session = session;
  }

  poll(): Promise<string> {
    return new Promise((resolve, reject) => {
      let event: any = {};
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