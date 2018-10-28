const restify = require('restify');
const errs = require('restify-errors');
const debug = require('debug')('engine-server');
const verbose = require('debug')('engine-server-verbose');
const Session = require('./session.js');
const EventStream = require('./event_stream.js');

const sessions = {}; // Should be a persistent store...
const eventStreams = {};

class ChannelEngine {
  constructor(assetMgr, options) {
    if (options && options.adCopyMgrUri) {
      this.adCopyMgrUri = options.adCopyMgrUri;
    }
    if (options && options.adXchangeUri) {
      this.adXchangeUri = options.adXchangeUri;
    }
    this.assetMgr = assetMgr;

    this.server = restify.createServer();
    this.server.use(restify.plugins.queryParser());
    
    if (options && options.staticDirectory) {
      this.server.get(/^\/$/, restify.plugins.serveStatic({
        directory: options.staticDirectory,
        default: 'index.html'
      }));
    }
    this.server.get('/live/master.m3u8', this._handleMasterManifest.bind(this));
    this.server.get(/^\/live\/master(\d+).m3u8;session=(.*)$/, this._handleMediaManifest.bind(this));
    this.server.get(/^\/live\/master-(\S+).m3u8;session=(.*)$/, this._handleAudioManifest.bind(this));
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
    let options = {};
    if (req.query['playlist']) {
      // Backward compatibility
      options.category = req.query['playlist'];
    }
    if (req.query['category']) {
      options.category = req.query['category'];
    }
    if (req.query['session'] && sessions[req.query['session']]) {
      session = sessions[req.query['session']];
    } else {
      if (req.query['startWithId']) {
        options.startWithId = req.query['startWithId'];
        debug(`New session to start with assetId=${options.startWithId}`);
      }
      options.adCopyMgrUri = this.adCopyMgrUri;
      options.adXchangeUri = this.adXchangeUri;
      session = new Session(this.assetMgr, options);
      sessions[session.sessionId] = session;
    }
    const eventStream = new EventStream(session);
    eventStreams[session.sessionId] = eventStream;

    session.getMasterManifest().then(body => {
      res.sendRaw(200, body, { 
        "Content-Type": "application/x-mpegURL",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "X-Session-Id",
        "Access-Control-Expose-Headers": "X-Session-Id",
        "Cache-Control": "no-cache",
        "X-Session-Id": session.sessionId,
      });
      next();
    }).catch(err => {
      next(this._errorHandler(err));
    });    
  }

  _handleAudioManifest(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params[1]];
    if (session) {
      session.getAudioManifest(req.params[0]).then(body => {
        verbose(`[${session.sessionId}] body=`);
        verbose(body);
        res.sendRaw(200, body, {
          "Content-Type": "application/x-mpegURL",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "max-age=4",
        });
        next();
      }).catch(err => {
        next(this._gracefulErrorHandler(err));
      });
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
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
        next(this._gracefulErrorHandler(err));
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

  _gracefulErrorHandler(errMsg) {
    console.error(errMsg);
    const err = new errs.NotFoundError(errMsg);
    return err;
  }

  _errorHandler(errMsg) {
    console.error(errMsg);
    const err = new errs.InternalServerError(errMsg);
    return err;    
  }
}

module.exports = ChannelEngine;
