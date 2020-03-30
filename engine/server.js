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
    this.useDemuxedAudio = false;
    if (options && options.demuxedAudio === true) {
      this.useDemuxedAudio = true;
    }
    this.assetMgr = assetMgr;

    this.server = restify.createServer();
    this.server.use(restify.plugins.queryParser());
    
    if (options && options.staticDirectory) {
      this.server.get('/', restify.plugins.serveStatic({
        directory: options.staticDirectory,
        default: 'index.html'
      }));
    }
    this.streamerOpts = {};
    if (options && options.averageSegmentDuration) {
      this.streamerOpts.averageSegmentDuration = options.averageSegmentDuration;
    }
    this.server.get('/live/:file', (req, res, next) => {
      debug(req.params);
      let m;
      if (req.params.file.match(/master.m3u8/)) {
        this._handleMasterManifest(req, res, next);
      } else if (m = req.params.file.match(/master(\d+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        this._handleMediaManifest(req, res, next);
      } else if (m = req.params.file.match(/master-(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        this._handleAudioManifest(req, res, next);
      }
    });
    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
    this.server.get('/status/:sessionId', this._handleStatus.bind(this));

    if (options && options.heartbeat) {
      this.server.get(options.heartbeat, this._handleHeartbeat.bind(this));
    }

    if (options && options.channelManager) {
      const channels = options.channelManager.getChannels();
      channels.map(channel => {
        if (!sessions[channel.id]) {
          sessions[channel.id] = new Session(this.assetMgr, {
            sessionId: channel.id,
            averageSegmentDuration: options.averageSegmentDuration,
            demuxedAudio: options.demuxedAudio,
            profile: channel.profile
          });
        }
      });
    }
  }

  start() {
    Object.keys(sessions).map(channelId => {
      const session = sessions[channelId];
      session.startPlayhead();
      setInterval(() => {
        session.getStatus().then(status => {
          debug(`MONITOR (${new Date().toISOString()}) [${status.sessionId}]: playhead: ${status.playhead.state}`);
          if (status.playhead.state === 'crashed') {
            debug(`[${status.sessionId}]: Playhead crashed, restarting`);
            session.restartPlayhead();
          }
        });
      }, 5000);
    });

  }

  listen(port) {
    this.server.listen(port, () => {
      debug('%s listening at %s', this.server.name, this.server.url);
    });
  }

  _handleHeartbeat(req, res, next) {
    debug('req.url=' + req.url);
    res.send(200);
    next();
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
    if (req.query['channel'] && sessions[req.query['channel']]) {
      session = sessions[req.query['channel']];
    } else if (req.query['session'] && sessions[req.query['session']]) {
      session = sessions[req.query['session']];
    } else {
      options.adCopyMgrUri = this.adCopyMgrUri;
      options.adXchangeUri = this.adXchangeUri;
      options.averageSegmentDuration = this.streamerOpts.averageSegmentDuration;
      options.useDemuxedAudio = this.useDemuxedAudio;
      session = new Session(this.assetMgr, options);
      sessions[session.sessionId] = session;
    }
    if (req.query['startWithId']) {
      options.startWithId = req.query['startWithId'];
      debug(`New session to start with assetId=${options.startWithId}`);
    }
    if (session) {
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
    } else {
      next(this._gracefulErrorHandler("Could not find a valid session"));
    } 
  }

  _handleAudioManifest(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params[1]];
    if (session) {
      session.getCurrentAudioManifest(req.params[0], req.headers["x-playback-session-id"]).then(body => {
        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
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
    debug(`${req.headers["x-playback-session-id"]} req.url=${req.url}`);
    debug(req.params);
    const session = sessions[req.params[1]];
    if (session) {
      session.getCurrentMediaManifest(req.params[0], req.headers["x-playback-session-id"]).then(body => {
        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
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

  _handleStatus(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params.sessionId];
    if (session) {
      session.getStatus().then(body => {
        res.send(200, body);
        next();
      });
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
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
