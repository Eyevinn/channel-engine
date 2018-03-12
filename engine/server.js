const restify = require('restify');
const errs = require('restify-errors');
const debug = require('debug')('engine-server');
const Session = require('./session.js');
const EventStream = require('./event_stream.js');

const sessions = {}; // Should be a persistent store...
const eventStreams = {};

class ChannelEngine {
  constructor(assetMgrUri, adCopyMgrUri) {
    this.server = restify.createServer();
    this.assetMgrUri = assetMgrUri;
    this.adCopyMgrUri = adCopyMgrUri;
    this.server.use(restify.plugins.queryParser());

    this.server.get('/live/master.m3u8', this._handleMasterManifest.bind(this));
    this.server.get(/^\/live\/master(\d+).m3u8;session=(.*)$/, this._handleMediaManifest.bind(this));
    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
  }

  listen(port) {
    this.server.listen(port, () => {
      debug('%s listening at %s', this.server.name, this.server.url);
    });
  }

  _handleMasterManifest(req, res, next) {
    debug('req.url=' + req.url);
    debug(req.query);
    let session;
    let playlist = 'random';
    if (req.query['playlist']) {
      playlist = req.query['playlist'];
    }
    if (req.query['session'] && sessions[req.query['session']]) {
      session = sessions[req.query['session']];
      if (session.currentPlaylist !== playlist) {
        session = new Session(this.assetMgrUri, this.adCopyMgrUri, playlist);
        sessions[session.sessionId] = session;
      }
    } else {
      let startWithId;
      if (req.query['startWithId']) {
        startWithId = req.query['startWithId'];
        debug(`New session to start with assetId=${startWithId}`);
      }
      session = new Session(this.assetMgrUri, this.adCopyMgrUri, playlist, startWithId);
      sessions[session.sessionId] = session;
    }
    const eventStream = new EventStream(session);
    eventStreams[session.sessionId] = eventStream;

    session.getMasterManifest().then(body => {
      debug(`[${session.sessionId}] body=`);
      debug(body);
      res.sendRaw(200, body, { 
        "Content-Type": "application/x-mpegURL",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "X-Session-Id",
        "Access-Control-Expose-Headers": "X-Session-Id",
        "Cache-Control": "max-age=4",
        "X-Session-Id": session.sessionId,
      });
      next();
    }).catch(err => {
      next(this._errorHandler(err));
    });    
  }

  _handleMediaManifest(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params[1]];
    if (session) {
      session.getMediaManifest(req.params[0]).then(body => {
        debug(`[${session.sessionId}] body=`);
        debug(body);
        res.sendRaw(200, body, { 
          "Content-Type": "application/x-mpegURL",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "max-age=4",
        });
        next();
      }).catch(err => {
        next(this._errorHandler(err));
      })
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
  }

  _handleEventStream(req, res, next) {
    debug(`req.url=${req.url}`);
    const eventStream = eventStreams[req.params.sessionId];
    if (eventStream) {
      eventStream.poll().then(body => {
        res.sendRaw(200, body, { 
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "max-age=4",
        });
        next();
      }).catch(err => {
        next(this._errorHandler(err));
      });
    } else {
      // Silent error
      debug(`No event stream found for session=${req.params.sessionId}`);
      res.sendRaw(200, '{}', { 
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "max-age=4",
      });
      next();
    } 
  }

  _errorHandler(errMsg) {
    console.error(errMsg);
    const err = new errs.InternalServerError(errMsg);
    return err;    
  }
}

module.exports = ChannelEngine;
