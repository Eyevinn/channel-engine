const restify = require('restify');
const errs = require('restify-errors');
const debug = require('debug')('engine-server');
const verbose = require('debug')('engine-server-verbose');
const Session = require('./session.js');
const EventStream = require('./event_stream.js');

const { SessionStateStore } = require('./session_state.js');
const { PlayheadStateStore } = require('./playhead_state.js');
const { filterQueryParser } = require('./util.js');

const sessions = {}; // Should be a persistent store...
const eventStreams = {};

class ChannelEngine {
  constructor(assetMgr, options) {
    this.options = options;
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
    if (options && options.defaultSlateUri) {
      this.defaultSlateUri = options.defaultSlateUri;
      this.slateRepetitions = options.slateRepetitions || 10;
      this.slateDuration = options.slateDuration || 4000;
    }
    this.assetMgr = assetMgr;
    this.monitorTimer = {};

    this.server = restify.createServer();
    this.server.use(restify.plugins.queryParser());

    this.sessionStore = {
      sessionStateStore: new SessionStateStore({ redisUrl: options.redisUrl }),
      playheadStateStore: new PlayheadStateStore({ redisUrl: options.redisUrl })
    };

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
    if (options && options.cacheTTL) {
      this.streamerOpts.cacheTTL = options.cacheTTL;
    }
    this.logCloudWatchMetrics = false;
    if (options && options.cloudWatchMetrics) {
      this.logCloudWatchMetrics = true;
    }
    this.server.get('/live/:file', async (req, res, next) => {
      debug(req.params);
      let m;
      if (req.params.file.match(/master.m3u8/)) {
        await this._handleMasterManifest(req, res, next);
      } else if (m = req.params.file.match(/master(\d+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        await this._handleMediaManifest(req, res, next);
      } else if (m = req.params.file.match(/master-(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        await this._handleAudioManifest(req, res, next);
      }
    });
    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
    this.server.get('/status/:sessionId', this._handleStatus.bind(this));
    this.server.get('/health/:sessionId', this._handleSessionHealth.bind(this));

    if (options && options.heartbeat) {
      this.server.get(options.heartbeat, this._handleHeartbeat.bind(this));
    }

    if (options && options.channelManager) {
      const t = setInterval(async () => { await this.updateChannelsAsync(options.channelManager, options) }, 60 * 1000);
    }
  }

  async updateChannelsAsync(channelMgr, options) {
    debug(`Do we have any new channels?`);
    const newChannels = channelMgr.getChannels().filter(channel => !sessions[channel.id]);
    const addAsync = async (channel) => {
      debug(`Adding channel with ID ${channel.id}`);
      sessions[channel.id] = new Session(this.assetMgr, {
        sessionId: channel.id,
        averageSegmentDuration: options.averageSegmentDuration,
        demuxedAudio: options.demuxedAudio,
        profile: channel.profile,
        slateUri: this.defaultSlateUri,
        slateRepetitions: this.slateRepetitions,
        slateDuration: this.slateDuration,
        cloudWatchMetrics: this.logCloudWatchMetrics,
      }, this.sessionStore);
      await sessions[channel.id].initAsync();
      if (!this.monitorTimer[channel.id]) {
        this.monitorTimer[channel.id] = setInterval(async () => { await this._monitorAsync(sessions[channel.id]) }, 5000);
      }
      await sessions[channel.id].startPlayheadAsync();
    };
    await Promise.all(newChannels.map(channel => addAsync(channel)));

    debug(`Have any channels been removed?`);
    const removedChannels = Object.keys(sessions).filter(channelId => !channelMgr.getChannels().find(ch => ch.id == channelId));
    const removeAsync = async (channelId) => {
      debug(`Removing channel with ID ${channelId}`);
      clearInterval(this.monitorTimer[channelId]);
      await sessions[channelId].stopPlayheadAsync();
      delete sessions[channelId];
    };
    await Promise.all(removedChannels.map(channelId => removeAsync(channelId)));
  }

  start() {
    const startAsync = async (channelId) => {
      const session = sessions[channelId];
      if (!this.monitorTimer[channelId]) {
        this.monitorTimer[channelId] = setInterval(async () => { await this._monitorAsync(session) }, 5000);
      }
      await session.startPlayheadAsync();
    };
    (async () => {
      debug("Starting engine");
      await this.updateChannelsAsync(this.options.channelManager, this.options);
      await Promise.all(Object.keys(sessions).map(channelId => startAsync(channelId)));
    })();
  }

  listen(port) {
    this.server.listen(port, () => {
      debug('%s listening at %s', this.server.name, this.server.url);
    });
  }

  async getStatusForSessionAsync(sessionId) {
    return await sessions[sessionId].getStatusAsync();
  }

  getSessionCount() {
    return Object.keys(sessions).length;
  }

  async _monitorAsync(session) {
    const status = await session.getStatusAsync();
    debug(`MONITOR (${new Date().toISOString()}) [${status.sessionId}]: playhead: ${status.playhead.state}`);
    if (status.playhead.state === 'crashed') {
      debug(`[${status.sessionId}]: Playhead crashed, restarting`);
      await session.restartPlayheadAsync();
    } else if (status.playhead.state === 'idle') {
      debug(`[${status.sessionId}]: Starting playhead`);       
      await session.startPlayheadAsync();
    }
  }

  _handleHeartbeat(req, res, next) {
    debug('req.url=' + req.url);
    res.send(200);
    next();
  }

  async _handleMasterManifest(req, res, next) {
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
      session = new Session(this.assetMgr, options, this.sessionStore);
      sessions[session.sessionId] = session;
    }
    if (req.query['startWithId']) {
      options.startWithId = req.query['startWithId'];
      debug(`New session to start with assetId=${options.startWithId}`);
    }
    if (session) {
      const eventStream = new EventStream(session);
      eventStreams[session.sessionId] = eventStream;

      let filter;
      if (req.query['filter']) {
        debug(`Applying filter on master manifest ${req.query['filter']}`);
        filter = filterQueryParser(req.query['filter']);
      }

      try {
        const body = await session.getMasterManifestAsync(filter);
        res.sendRaw(200, body, { 
          "Content-Type": "application/x-mpegURL",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "X-Session-Id",
          "Access-Control-Expose-Headers": "X-Session-Id",
          "Cache-Control": "no-cache",
          "X-Session-Id": session.sessionId,
        });
        next();
      } catch (err) {
        next(this._errorHandler(err));
      }
    } else {
      next(this._gracefulErrorHandler("Could not find a valid session"));
    } 
  }

  async _handleAudioManifest(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params[1]];
    if (session) {
      try {
        const body = await session.getCurrentAudioManifestAsync(req.params[0], req.headers["x-playback-session-id"]);
        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
        res.sendRaw(200, body, {
          "Content-Type": "application/x-mpegURL",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        });
        next();
      } catch (err) {
        next(this._gracefulErrorHandler(err));
      }
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
  }

  async _handleMediaManifest(req, res, next) {
    debug(`${req.headers["x-playback-session-id"]} req.url=${req.url}`);
    debug(req.params);
    const session = sessions[req.params[1]];
    if (session) {
      try {
        const body = await session.getCurrentMediaManifestAsync(req.params[0], req.headers["x-playback-session-id"]);
        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
        res.sendRaw(200, body, { 
          "Content-Type": "application/x-mpegURL",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        });
        next();
      } catch (err) {
        next(this._gracefulErrorHandler(err));
      }
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

  async _handleStatus(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params.sessionId];
    if (session) {
      const body = await session.getStatusAsync();
      res.send(200, body);
      next();
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
  }

  async _handleSessionHealth(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params.sessionId];
    if (session) {
      const status = await session.getStatusAsync();
      if (status.playhead && status.playhead.state === "running") {
        res.send(200, { "health": "ok", "tick": status.playhead.tickMs });
      } else {
        res.send(503, { "health": "unhealthy" });
      }
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
