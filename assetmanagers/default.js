const request = require('request');

class AssetManager {
  constructor(assetMgrUri) {
    this._assetMgrUri = assetMgrUri;
    this._sessions = {};
  }

  getNextVod(sessionId, category) {
    return new Promise((resolve, reject) => {
      if (!this._sessions[sessionId]) {
        this._sessions[sessionId] = {
          position: 0,
          playlist: category || 'random',
        }
      }
      this._sessions[sessionId].position++;
      const nextVodUri = this._assetMgrUri + '/nextVod/' + this._sessions[sessionId].playlist + '?position=' + this._sessions[sessionId].position;
      request.get(nextVodUri, (err, resp, body) => {
        const data = JSON.parse(body);
        if (data.playlistPosition !== undefined) {
          this._sessions[sessionId].position = data.playlistPosition;
        }
        this.currentMetadata = {
          id: data.id,
          title: data.title || '',
        };
        resolve({ id: data.id, title: data.title || '', uri: data.uri });
      }).on('error', err => {
        reject(err);
      });
    });
  }
}

module.exports = AssetManager;