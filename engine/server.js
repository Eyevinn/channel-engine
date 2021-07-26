const restify = require('restify');
const errs = require('restify-errors');
const { v4: uuidv4 } = require('uuid');
const debug = require('debug')('engine-server');
const verbose = require('debug')('engine-server-verbose');
const Session = require('./session.js');
const SessionLive = require('./session_live.js');
const StreamSwitcher = require('./stream_switcher.js');
const EventStream = require('./event_stream.js');

const { SessionStateStore } = require('./session_state.js');
const { PlayheadStateStore } = require('./playhead_state.js');
const { filterQueryParser, toHHMMSS } = require('./util.js');
const { version } = require('../package.json');

const sessions = {}; // Should be a persistent store...
const sessionsLive = {}; // Should be a persistent store...
const sessionSwitcher = {};
const eventStreams = {};
const switchSession = {};

const timer = ms => new Promise(res => setTimeout(res, ms));

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
    if (options && options.useDemuxedAudio === true) {
      this.useDemuxedAudio = true;
    }
    if (options && options.defaultSlateUri) {
      this.defaultSlateUri = options.defaultSlateUri;
      this.slateRepetitions = options.slateRepetitions || 10;
      this.slateDuration = options.slateDuration || 4000;
    }
    if (options && options.streamSwitchManager) {
      this.streamSwitchManager = options.streamSwitchManager;
    }
    this.assetMgr = assetMgr;
    this.monitorTimer = {};
    this.server = restify.createServer();
    this.server.use(restify.plugins.queryParser());
    this.serverStartTime = Date.now();
    this.instanceId = uuidv4();

    this.sessionStore = {
      sessionStateStore: new SessionStateStore({ 
        redisUrl: options.redisUrl, 
        memcachedUrl: options.memcachedUrl, 
        cacheTTL: options.sharedStoreCacheTTL,
      }),
      playheadStateStore: new PlayheadStateStore({ 
        redisUrl: options.redisUrl, 
        memcachedUrl: options.memcachedUrl, 
        cacheTTL: options.sharedStoreCacheTTL,
      }),
      instanceId: this.instanceId,
    };

    if (options && options.staticDirectory) {
      this.server.get('/', restify.plugins.serveStatic({
        directory: options.staticDirectory,
        default: 'index.html'
      }));
    }
    this.streamerOpts = {};
    if (options && options.averageSegmentDuration) {
      this.streamerOpts.defaultAverageSegmentDuration = options.averageSegmentDuration;
    }
    if (options && options.cacheTTL) {
      this.streamerOpts.cacheTTL = options.cacheTTL;
    }
    this.logCloudWatchMetrics = false;
    if (options && options.cloudWatchMetrics) {
      this.logCloudWatchMetrics = true;
    }
    if (options && options.playheadDiffThreshold) {
      this.streamerOpts.defaultPlayheadDiffThreshold = options.playheadDiffThreshold;
    }
    if (options && options.maxTickInterval) {
      this.streamerOpts.defaultMaxTickInterval = options.maxTickInterval;
    }
    if (options && options.targetDurationPadding) {
      this.streamerOpts.targetDurationPadding = options.targetDurationPadding;
    }
    if (options && options.forceTargetDuration) {
      this.streamerOpts.forceTargetDuration = options.forceTargetDuration;
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
      } else if (m = req.params.file.match(/master-(\S+)_(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        req.params[2] = m[3];
        await this._handleAudioManifest(req, res, next);
      }
    });
    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
    this.server.get('/status/:sessionId', this._handleStatus.bind(this));
    this.server.get('/health', this._handleAggregatedSessionHealth.bind(this));
    this.server.get('/health/:sessionId', this._handleSessionHealth.bind(this));
    this.server.get('/reset', this._handleSessionReset.bind(this));

    this.server.on('NotFound', (req, res, err, next) => {
      res.header("X-Instance-Id", this.instanceId + `<${version}>`);
      return next();
    });
    this.server.on('InternalServer', (req, res, err, next) => {
      res.header("X-Instance-Id", this.instanceId + `<${version}>`);
      return next();
    });

    if (options && options.heartbeat) {
      this.server.get(options.heartbeat, this._handleHeartbeat.bind(this));
    }

    if (options && options.channelManager) {
      const t = setInterval(async () => { await this.updateChannelsAsync(options.channelManager, options) }, 60 * 1000);
    }

    const pingSessionSwitcher = setInterval(async () => {
      for (const chId in sessionSwitcher) {
        if (Object.hasOwnProperty.call(sessionSwitcher, chId)) {
          const switcher = sessionSwitcher[chId];
          switchSession[chId] = null;
          switchSession[chId] = await switcher.streamSwitcher(sessions[chId], sessionsLive[chId]);
          // console.log(`Instance: [${this.instanceId}] Channel: [${chId}] TIME: [${Date.now()}]`)
        }
      }
    }, 2000);

    const ping = setInterval(async () => { await this.sessionStore.sessionStateStore.ping(this.instanceId); }, 3000);
  }

  async updateChannelsAsync(channelMgr, options) {
    debug(`Do we have any new channels?`);
    const newChannels = channelMgr.getChannels().filter(channel => !sessions[channel.id]);
    const addAsync = async (channel) => {
      debug(`Adding channel with ID ${channel.id}`);
      sessions[channel.id] = new Session(this.assetMgr, {
        sessionId: channel.id,
        averageSegmentDuration: channel.options && channel.options.averageSegmentDuration ? channel.options.averageSegmentDuration : this.streamerOpts.defaultAverageSegmentDuration,
        useDemuxedAudio: options.useDemuxedAudio,
        playheadDiffThreshold: channel.options && channel.options.playheadDiffThreshold ? channel.options.playheadDiffThreshold : this.streamerOpts.defaultPlayheadDiffThreshold,
        maxTickInterval: channel.options && channel.options.maxTickInterval ? channel.options.maxTickInterval : this.streamerOpts.defaultMaxTickInterval,
        targetDurationPadding: channel.options && channel.options.targetDurationPadding ? channel.options.targetDurationPadding : this.streamerOpts.targetDurationPadding,
        forceTargetDuration: channel.options && channel.options.forceTargetDuration ? channel.options.forceTargetDuration : this.streamerOpts.forceTargetDuration,
        profile: channel.profile,
        audioTracks: channel.audioTracks,
        closedCaptions: channel.closedCaptions,
        slateUri: channel.slate && channel.slate.uri ? channel.slate.uri : this.defaultSlateUri,
        slateRepetitions: channel.slate && channel.slate.repetitions ? channel.slate.repetitions : this.slateRepetitions,
        slateDuration: channel.slate && channel.slate.duration ? channel.slate.duration : this.slateDuration,
        cloudWatchMetrics: this.logCloudWatchMetrics,
      }, this.sessionStore);

      sessionsLive[channel.id] = new SessionLive({
        instanceId: this.sessionStore.instanceId,
        sessionId: channel.id,
        useDemuxedAudio: options.useDemuxedAudio,
        cloudWatchMetrics: this.logCloudWatchMetrics,
      });

      sessionSwitcher[channel.id] = new StreamSwitcher({
        sessionId: channel.id,
        useDemuxedAudio: options.useDemuxedAudio,
        cloudWatchMetrics: this.logCloudWatchMetrics,
        streamSwitchManager: this.streamSwitchManager,
      });

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
      delete sessionsLive[channelId];
      delete sessionSwitcher[channelId];
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

  getPlayheadCount() {
    return Object.keys(sessions).filter(sessionId => sessions[sessionId].hasPlayhead()).length;
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
      options.averageSegmentDuration = this.streamerOpts.defaultAverageSegmentDuration;
      options.useDemuxedAudio = this.useDemuxedAudio;
      options.playheadDiffThreshold = this.streamerOpts.defaultPlayheadDiffThreshold;
      options.maxTickInterval = this.streamerOpts.defaultMaxTickInterval;
      options.targetDurationPadding = this.streamerOpts.targetDurationPadding;
      options.forceTargetDuration = this.streamerOpts.forceTargetDuration;
      // if we are initiating a master manifest
      // outside of specific Channel context,
      // if slate options are set at the ChannelEngine level, then set these here
      if (this.defaultSlateUri) {
        options.slateUri = this.defaultSlateUri;
      }
      if (this.slateRepetitions) {
        options.slateRepetitions = this.slateRepetitions;
      }
      if (this.slateDuration) {
        options.slateDuration = this.slateDuration;
      }
      options.disabledPlayhead = true; // disable playhead for personalized streams
      session = new Session(this.assetMgr, options, this.sessionStore);
      await session.initAsync();
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
        res.sendRaw(200, Buffer.from(body, 'utf8'), {
          "Content-Type": "application/x-mpegURL;charset=UTF-8",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "X-Session-Id",
          "Access-Control-Expose-Headers": "X-Session-Id",
          "Cache-Control": "no-cache",
          "X-Session-Id": session.sessionId,
          "X-Instance-Id": this.instanceId + `<${version}>`,
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
    const session = sessions[req.params[2]];
    if (session) {
      try {
        const body = await session.getCurrentAudioManifestAsync(
          req.params[0],
          req.params[1],
          req.headers["x-playback-session-id"]
        );
        res.sendRaw(200, Buffer.from(body, 'utf8'), {
          "Content-Type": "application/x-mpegURL;charset=UTF-8",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
          "X-Instance-Id": this.instanceId + `<${version}>`,
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
    debug(`x-playback-session-id=${req.headers["x-playback-session-id"]} req.url=${req.url}`);
    debug(req.params);
    const session = sessions[req.params[1]];
    const sessionLive = sessionsLive[req.params[1]]; 
    if (session && sessionLive) {
      try {
        while(switchSession[req.params[1]] === null){
          console.log(`[${req.params[1]}]: Waiting for streamSwitcher to finish switching session`);
          await timer(500);
        }
        let body = null;
        if (switchSession[req.params[1]]) {
          debug(`[${req.params[1]}]: Responding with Altered-Live stream manifest`);
          body = await sessionLive.getCurrentMediaManifestAsync(req.params[0]);
        } else {
          debug(`[${req.params[1]}]: Responding with VOD2Live stream manifest`);
          body = await session.getCurrentMediaManifestAsync(req.params[0], req.headers["x-playback-session-id"]);
        }
        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
        res.sendRaw(200, Buffer.from(body, 'utf8'), { 
          "Content-Type": "application/x-mpegURL;charset=UTF-8",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
          "X-Instance-Id": this.instanceId + `<${version}>`,
        });
        next();
      } catch (err) {
        next(this._gracefulErrorHandler(err));
      }
    } else {
      const err = new errs.NotFoundError('Invalid session(s)');
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
      res.sendRaw(200, JSON.stringify(body), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
      next();
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
  }

  async _handleAggregatedSessionHealth(req, res, next) {
    debug(`req.url=${req.url}`);
    let failingSessions = [];
    let endpoints = [];
    for (const sessionId of Object.keys(sessions)) {
      const session = sessions[sessionId];
      if (session && session.hasPlayhead()) {
        const status = await session.getStatusAsync();
        if (status.playhead && status.playhead.state !== "running") {
          failingSessions.push(status);
        }
        endpoints.push({
          health: '/health/' + sessionId,
          status: '/status/' + sessionId,
          playback: '/live/master.m3u8?channel=' + sessionId,
        });
      }
    }
    const engineStatus = {
      startTime: new Date(this.serverStartTime).toISOString(),
      uptime: toHHMMSS((Date.now() - this.serverStartTime) / 1000),
      version: version,
      instanceId: this.instanceId,
    };
    if (failingSessions.length === 0) {
      res.sendRaw(200, 
        JSON.stringify({ "health": "ok", "engine": engineStatus, "count": endpoints.length, "sessionEndpoints": endpoints }),
        {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",  
        });
    } else {
      res.sendRaw(503, JSON.stringify({ "health": "unhealthy", "engine": engineStatus, "failed": failingSessions }),
      {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
    }
  }

  async _handleSessionHealth(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params.sessionId];
    if (session) {
      const status = await session.getStatusAsync();
      if (status.playhead && status.playhead.state === "running") {
        res.sendRaw(200, JSON.stringify({ "health": "ok", "tick": status.playhead.tickMs, "mediaSeq": status.playhead.mediaSeq }),
        {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",  
        });
      } else {
        res.sendRaw(503, JSON.stringify({ "health": "unhealthy" }),
        {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",
        });
      }
    } else {
      const err = new errs.NotFoundError('Invalid session');
      next(err);
    }
  }

  async _handleSessionReset(req, res, next) {
    debug(`req.url=${req.url}`);
    let sessionResets = [];
    for (const sessionId of Object.keys(sessions)) {
      const session = sessions[sessionId];
      if (session) {
        await session.resetAsync();
        sessionResets.push(sessionId);
      } else {
        const err = new errs.NotFoundError('Invalid session');
        next(err);
      }
    }
    res.sendRaw(200, JSON.stringify({ "status": "ok", "instanceId": this.instanceId, "resets": sessionResets }),
    {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-cache",
    });
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
