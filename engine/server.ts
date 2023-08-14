const restify = require('restify');
const errs = require('restify-errors');
const { v4: uuidv4 } = require('uuid');
const debug = require('debug')('engine-server');
const verbose = require('debug')('engine-server-verbose');
const Session = require('./session.js');
const SessionLive = require('./session_live.js');
const StreamSwitcher = require('./stream_switcher.js');
const EventStream = require('./event_stream.js');
const SubtitleSlicer = require('./subtitle_slicer.js');
const { timer }= require('./util.js');

const { SessionStateStore } = require('./session_state.js');
const { SessionLiveStateStore } = require('./session_live_state.js');
const { PlayheadStateStore } = require('./playhead_state.js');

const { filterQueryParser, toHHMMSS, WaitTimeGenerator } = require('./util.js');
const { version } = require('../package.json');

const AUTO_CREATE_CHANNEL_TIMEOUT = 3000;

const sessions = {}; // Should be a persistent store...
const sessionsLive = {}; // Should be a persistent store...
const sessionSwitchers = {}; // Should be a persistent store...
const switcherStatus = {}; // Should be a persistent store...
const eventStreams = {};
const DefaultDummySubtitleEndpointPath = "/vtt/dummyUrl"
const DefaultSubtitleSpliceEndpointPath = "/vtt/sliceUrl"

export interface ChannelEngineOpts {
  defaultSlateUri?: string;
  slateRepetitions?: number;
  slateDuration?: number;
  redisUrl?: string;
  memcachedUrl?: string;
  sharedStoreCacheTTL?: number;
  heartbeat?: string;
  channelManager: IChannelManager;
  streamSwitchManager?: IStreamSwitchManager;
  cacheTTL?: number;
  playheadDiffThreshold?: number;
  maxTickInterval?: number;
  cloudWatchMetrics?: boolean;
  useDemuxedAudio?: boolean;
  dummySubtitleEndpoint?: string;
  subtitleSliceEndpoint?: string;
  useVTTSubtitles?: boolean;
  alwaysNewSegments?: boolean;
  partialStoreHLSVod?: boolean;
  alwaysMapBandwidthByNearest?: boolean;
  diffCompensationRate?: number;
  staticDirectory?: string;
  averageSegmentDuration?: number;
  targetDurationPadding?: boolean;
  forceTargetDuration?: boolean;
  adCopyMgrUri?: string; // deprecated
  adXchangeUri?: string; // deprecated
  noSessionDataTags?: boolean;
  volatileKeyTTL?: number;
  autoCreateSession?: boolean;
}

interface StreamerOpts {
  defaultAverageSegmentDuration?: number;
  cacheTTL?: number;
  defaultPlayheadDiffThreshold?: number;
  defaultMaxTickInterval?: number;
  targetDurationPadding?: boolean;
  forceTargetDuration?: boolean;
  diffCompensationRate?: number;
}

export interface VodRequest {
  sessionId: string;
  category?: string;
  playlistId: string;
}

export interface VodResponseMetadata {
  id: string;
  title: string;
}

export interface VodTimedMetadata {
  'start-date': string;
  'x-schedule-end'?: string;
  'x-title'?: string;
  'x-channelid'?: string;
  'class': string;
}

export interface LiveTimedMetadata {
  'id': string;
  'start-date': string;
  'x-title'?: string;
}

export interface VodResponse {
  title: any;
  id: string;
  uri: string;
  offset?: number;
  diffMs?: number;
  desiredDuration?: number;
  startOffset?: number;
  type?: string;
  currentMetadata?: VodResponseMetadata;
  timedMetadata?: VodTimedMetadata;
  // Wall-clock start time of the VOD as unix ts. If set
  // the program-date-time will be added to each segment
  unixTs?: number;
}

export interface IAssetManager {
  getNextVod: (vodRequest: VodRequest) => Promise<VodResponse>;
  handleError?: (err: string, vodResponse: VodResponse) => void;
}

export interface ChannelProfile {
  bw: number;
  codecs: string;
  resolution: number[];
  channels?: string;
}

export interface Channel {
  id: string;
  profile: ChannelProfile[];
  audioTracks?: AudioTracks[];
  subtitleTracks?: SubtitleTracks[];
  closedCaptions?: ClosedCaptions[];
}

export interface ClosedCaptions {
  id: string;
  lang: string;
  name: string;
  default?: boolean;
  auto?: boolean;
}

export interface AudioTracks {
  language: string;
  name: string;
  default?: boolean;
  enforceAudioGroupId?: string;
}
export interface SubtitleTracks {
  language: string;
  name: string;
  default?: boolean;
}

export interface IChannelManager {
  getChannels: () => Channel[];
  autoCreateChannel?: (channelId: string) => void;
}

export enum ScheduleStreamType {
  LIVE = 1,
  VOD = 2
};

export interface Schedule {
  eventId: string;
  assetId: string;
  title: string;
  type: ScheduleStreamType;
  start_time: number;
  end_time: number;
  uri: string;
  duration?: number;
  timedMetadata?: LiveTimedMetadata;
}

export interface IStreamSwitchManager {
  getSchedule: (channelId: string) => Promise<Schedule[]>;
}

export class ChannelEngine {
  private options?: ChannelEngineOpts;
  private useDemuxedAudio: boolean;
  private dummySubtitleEndpoint: string;
  private subtitleSliceEndpoint: string;
  private useVTTSubtitles: boolean;
  private alwaysNewSegments: boolean;
  private partialStoreHLSVod: boolean;
  private alwaysMapBandwidthByNearest: boolean;
  private defaultSlateUri?: string;
  private slateDuration?: number;
  private assetMgr: IAssetManager;
  private streamSwitchManager?: any;
  private slateRepetitions?: number;
  private monitorTimer: any;
  private server: any;
  private serverStartTime: number;
  private instanceId: string;
  private streamSwitchTimeIntervalMs: number;
  private sessionStore: any;
  private sessionLiveStore: any;
  private streamerOpts: StreamerOpts;
  private logCloudWatchMetrics: boolean;
  private adCopyMgrUri?: string;
  private adXchangeUri?: string;
  private autoCreateSession: boolean = false;
  
  constructor(assetMgr: IAssetManager, options?: ChannelEngineOpts) {
    this.options = options;
    if (options && options.adCopyMgrUri) {
      this.adCopyMgrUri = options.adCopyMgrUri;
    }
    if (options && options.adXchangeUri) {
      this.adXchangeUri = options.adXchangeUri;
    }
    this.useDemuxedAudio = false;
    if (options && options.useDemuxedAudio) {
      this.useDemuxedAudio = true;
    }

    this.useVTTSubtitles = (options && options.useVTTSubtitles) ? options.useVTTSubtitles : false ;
    this.dummySubtitleEndpoint = (options && options.dummySubtitleEndpoint) ? options.dummySubtitleEndpoint : DefaultDummySubtitleEndpointPath;
    this.subtitleSliceEndpoint = (options && options.subtitleSliceEndpoint) ? options.subtitleSliceEndpoint : DefaultSubtitleSpliceEndpointPath;

    this.alwaysNewSegments = false;
    if (options && options.alwaysNewSegments) {
      this.alwaysNewSegments = true;
    }
    this.partialStoreHLSVod = false;
    if (options && options.partialStoreHLSVod) {
      this.partialStoreHLSVod = true;
    }
    this.alwaysMapBandwidthByNearest = false;
    if (options && options.alwaysMapBandwidthByNearest) {
      this.alwaysMapBandwidthByNearest = true;
    }
    if (options && options.defaultSlateUri) {
      this.defaultSlateUri = options.defaultSlateUri;
      this.slateRepetitions = options.slateRepetitions || 10;
      this.slateDuration = options.slateDuration || 4000;
    }
    if (options && options.streamSwitchManager) {
      this.streamSwitchManager = options.streamSwitchManager;
    }
    if (options && options.autoCreateSession !== undefined) {
      this.autoCreateSession = options.autoCreateSession;
    }
    this.assetMgr = assetMgr;
    this.monitorTimer = {};
    this.server = restify.createServer();
    this.server.use(restify.plugins.queryParser());
    this.serverStartTime = Date.now();
    this.instanceId = uuidv4();

    this.streamSwitchTimeIntervalMs = 3000;

    this.sessionStore = {
      sessionStateStore: new SessionStateStore({
        redisUrl: options.redisUrl, 
        memcachedUrl: options.memcachedUrl, 
        cacheTTL: options.sharedStoreCacheTTL,
        volatileKeyTTL: options.volatileKeyTTL,
      }),
      playheadStateStore: new PlayheadStateStore({ 
        redisUrl: options.redisUrl, 
        memcachedUrl: options.memcachedUrl, 
        cacheTTL: options.sharedStoreCacheTTL,
        volatileKeyTTL: options.volatileKeyTTL,
      }),
      instanceId: this.instanceId,
    };

    this.sessionLiveStore = {
      sessionLiveStateStore: new SessionLiveStateStore({
        redisUrl: options.redisUrl, 
        memcachedUrl: options.memcachedUrl, 
        cacheTTL: options.sharedStoreCacheTTL,
        volatileKeyTTL: options.volatileKeyTTL,
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
    if (options && options.diffCompensationRate) {
      this.streamerOpts.diffCompensationRate = options.diffCompensationRate;
    }
    const handleMasterRoute = async (req, res, next) => {
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
      } else if (m = req.params.file.match(/subtitles-(\S+)_(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        req.params[2] = m[3];
        await this._handleSubtitleManifest(req, res, next);
      }
    };
    this.server.get('/live/:file', async (req, res, next) => {
      await handleMasterRoute(req, res, next);
    });
    this.server.get('/channels/:channelId/:file', async (req, res, next) => {
      req.query['channel'] = req.params.channelId;
      await handleMasterRoute(req, res, next);
    });   
    this.server.opts('/live/:file', async (req, res, next) => {
      res.sendRaw(204, "", {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
        },
      });
      next();
    });
    this.server.opts('/channels/:channelId/:file', async (req, res, next) => {
      res.sendRaw(204, "", {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
        },
      });
      next();
    });
    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
    this.server.get('/status/:sessionId', this._handleStatus.bind(this));
    this.server.get('/health', this._handleAggregatedSessionHealth.bind(this));
    this.server.get('/health/:sessionId', this._handleSessionHealth.bind(this));
    this.server.get('/reset', this._handleSessionReset.bind(this));
    this.server.get(DefaultDummySubtitleEndpointPath, this._handleDummySubtitleEndpoint.bind(this));
    this.server.get(DefaultSubtitleSpliceEndpointPath, this._handleSubtitleSliceEndpoint.bind(this));

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

    const LeaderSyncSessionTypes = async () => {
      await timer(10*1000);
      let isLeader = await this.sessionStore.sessionStateStore.isLeader(this.instanceId);
      if (isLeader) {
        await this.sessionLiveStore.sessionLiveStateStore.setLeader(this.instanceId);
      }
    }
    LeaderSyncSessionTypes();

    const ping = setInterval(async () => {
      await this.sessionStore.sessionStateStore.ping(this.instanceId);
      await this.sessionLiveStore.sessionLiveStateStore.ping(this.instanceId);
    }, 3000);

    const StreamSwitchLoop = async (timeIntervalMs) => {
      const minIntervalMs = 50;
      const WTG = new WaitTimeGenerator(timeIntervalMs, minIntervalMs);
      while(true) {
        try {
          const ts_1 = Date.now();
          await this.updateStreamSwitchAsync()
          const ts_2 = Date.now();
          const  interval = (timeIntervalMs - (ts_2 - ts_1)) < 0 ? minIntervalMs : (timeIntervalMs - (ts_2 - ts_1)); 
          const tickInterval = await WTG.getWaitTime(interval);
          await timer(tickInterval)
          debug(`StreamSwitchLoop waited for all channels. Next tick in: ${tickInterval}ms`)
        } catch (err) {
          console.error(err)
          debug(`StreamSwitchLoop iteration failed. Trying Again in 1000ms!`);
          await timer(1000);
        }
      }
    }
    if (this.streamSwitchManager) {
      StreamSwitchLoop(this.streamSwitchTimeIntervalMs);
    }
  }

  async updateStreamSwitchAsync() {
    const channels = Object.keys(sessionSwitchers);
    const getSwitchStatusAndPerformSwitch = async (channel) => {
      if (sessionSwitchers[channel]) {
        const switcher = sessionSwitchers[channel];
        let prevStatus = switcherStatus[channel] !== null ? switcherStatus[channel] : null;
        switcherStatus[channel] = null;
        let status = null;
        try {
          status = await switcher.streamSwitcher(sessions[channel], sessionsLive[channel]);
          debug(`[${channel}]: streamSwitcher returned switchstatus=${status}`);
          if (status === undefined) {
            debug(`[WARNING]: switcherStatus->${status}. Setting value to previous status->${prevStatus}`);
            status = prevStatus;
          }
          switcherStatus[channel] = status;
        } catch (err) {
          throw new Error (err);
        }
       } else {
        debug(`Tried to switch stream on a non-existing channel=[${channel}]. Switching Ignored!)`);
      }
    }
    try {
      await Promise.all(channels.map(channel => getSwitchStatusAndPerformSwitch(channel)));
    } catch (err) {
      debug('Problem occured when updating streamSwitchers');
      throw new Error (err);
    }

  }

  async updateChannelsAsync(channelMgr, options) {
    debug(`Do we have any new channels?`);
    const newChannels = channelMgr.getChannels().filter(channel => !sessions[channel.id]);
    debug(newChannels);
    const addAsync = async (channel) => {
      debug(`Adding channel with ID ${channel.id}`);
      sessions[channel.id] = new Session(this.assetMgr, {
        sessionId: channel.id,
        averageSegmentDuration: channel.options && channel.options.averageSegmentDuration ? channel.options.averageSegmentDuration : this.streamerOpts.defaultAverageSegmentDuration,
        useDemuxedAudio: options.useDemuxedAudio,
        dummySubtitleEndpoint: this.dummySubtitleEndpoint,
        subtitleSliceEndpoint: this.subtitleSliceEndpoint,
        useVTTSubtitles: this.useVTTSubtitles,
        alwaysNewSegments: options.alwaysNewSegments,
        partialStoreHLSVod: options.partialStoreHLSVod,
        alwaysMapBandwidthByNearest: options.alwaysMapBandwidthByNearest,
        noSessionDataTags: options.noSessionDataTags,
        playheadDiffThreshold: channel.options && channel.options.playheadDiffThreshold ? channel.options.playheadDiffThreshold : this.streamerOpts.defaultPlayheadDiffThreshold,
        maxTickInterval: channel.options && channel.options.maxTickInterval ? channel.options.maxTickInterval : this.streamerOpts.defaultMaxTickInterval,
        targetDurationPadding: channel.options && channel.options.targetDurationPadding ? channel.options.targetDurationPadding : this.streamerOpts.targetDurationPadding,
        forceTargetDuration: channel.options && channel.options.forceTargetDuration ? channel.options.forceTargetDuration : this.streamerOpts.forceTargetDuration,
        diffCompensationRate: channel.options && channel.options.diffCompensationRate ? channel.options.diffCompensationRate : this.streamerOpts.diffCompensationRate,
        profile: channel.profile,
        audioTracks: channel.audioTracks,
        subtitleTracks: channel.subtitleTracks,
        closedCaptions: channel.closedCaptions,
        slateUri: channel.slate && channel.slate.uri ? channel.slate.uri : this.defaultSlateUri,
        slateRepetitions: channel.slate && channel.slate.repetitions ? channel.slate.repetitions : this.slateRepetitions,
        slateDuration: channel.slate && channel.slate.duration ? channel.slate.duration : this.slateDuration,
        cloudWatchMetrics: this.logCloudWatchMetrics,
      }, this.sessionStore);

      sessionsLive[channel.id] = new SessionLive({
        sessionId: channel.id,
        useDemuxedAudio: options.useDemuxedAudio,
        dummySubtitleEndpoint: this.dummySubtitleEndpoint,
        subtitleSliceEndpoint: this.subtitleSliceEndpoint,
        useVTTSubtitles: this.useVTTSubtitles,
        cloudWatchMetrics: this.logCloudWatchMetrics,
        profile: channel.profile,
      }, this.sessionLiveStore);

      sessionSwitchers[channel.id] = new StreamSwitcher({
        sessionId: channel.id,
        streamSwitchManager: this.streamSwitchManager ? this.streamSwitchManager : null
      });

      await sessions[channel.id].initAsync();
      await sessionsLive[channel.id].initAsync();
      if (!this.monitorTimer[channel.id]) {
        this.monitorTimer[channel.id] = setInterval(async () => { await this._monitorAsync(sessions[channel.id], sessionsLive[channel.id]) }, 5000);
      }

      await sessions[channel.id].startPlayheadAsync();
    };

    const addLiveAsync = async (channel) => {
      debug(`Adding live channel with ID ${channel.id}`);
      await sessionsLive[channel.id].initAsync();
      await sessionsLive[channel.id].startPlayheadAsync();
    };
    await Promise.all(newChannels.map(channel => addAsync(channel)).concat(newChannels.map(channel => addLiveAsync(channel))));

    debug(`Have any channels been removed?`);
    const removedChannels = Object.keys(sessions).filter(channelId => !channelMgr.getChannels().find(ch => ch.id == channelId));
    const removeAsync = async (channelId) => {
      debug(`Removing channel with ID ${channelId}`);
      clearInterval(this.monitorTimer[channelId]);
      await sessions[channelId].stopPlayheadAsync();
      if (sessionsLive[channelId]) {
        await sessionsLive[channelId].stopPlayheadAsync();
        delete sessionsLive[channelId];
      } else {
        debug(`Cannot remove live session of channel that does not exist ${channelId}`);
      }
      delete sessions[channelId];
      delete sessionSwitchers[channelId];
      delete switcherStatus[channelId];
    };
    await Promise.all(removedChannels.map(channelId => removeAsync(channelId)));
  }

  start() {
    const startAsync = async (channelId) => {
      const session = sessions[channelId];
      const sessionLive = sessionsLive[channelId];
      if (!this.monitorTimer[channelId]) {
        this.monitorTimer[channelId] = setInterval(async () => { await this._monitorAsync(session, sessionLive) }, 5000);
      }
      session.startPlayheadAsync();
      await sessionLive.startPlayheadAsync();
    };
    const startLiveAsync = async (channelId) => {
      const sessionLive = sessionsLive[channelId];
      await sessionLive.startPlayheadAsync();
    };

    (async () => {
      debug("Starting engine");
      await this.updateChannelsAsync(this.options.channelManager, this.options);
      await Promise.all(Object.keys(sessions).map(channelId => startAsync(channelId)).concat(Object.keys(sessionsLive).map(channelId => startLiveAsync(channelId))));
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

  async _monitorAsync(session, sessionLive) {
    const statusSessionLive = sessionLive.getStatus();
    const statusSession = await session.getStatusAsync();

    debug(`MONITOR: (${new Date().toISOString()}) [${statusSession.sessionId}]: playhead: ${statusSession.playhead.state}`);
    debug(`MONITOR: (${new Date().toISOString()}) [${statusSessionLive.sessionId}]: live-playhead: ${statusSessionLive.playhead.state}`);

    if (statusSessionLive.playhead.state === 'crashed') {
      debug(`[${statusSessionLive.sessionId}]: SessionLive-Playhead crashed, restarting`);
      await sessionLive.restartPlayheadAsync();
    }
    if (statusSession.playhead.state === 'crashed') {
      debug(`[${statusSession.sessionId}]: Session-Playhead crashed, restarting`);
      await session.restartPlayheadAsync();
    } else if (statusSession.playhead.state === 'idle') {
      debug(`[${statusSession.sessionId}]: Starting playhead`);
      await session.startPlayheadAsync();
    }
  }

  async createChannel(channelId) {
    if (!sessions[channelId]) {
      if (this.options.channelManager.autoCreateChannel) {
        this.options.channelManager.autoCreateChannel(channelId);
        setTimeout(async () => { await this.updateChannelsAsync(this.options.channelManager, this.options) });
        await timer(AUTO_CREATE_CHANNEL_TIMEOUT);
      }  
    }
  }

  async getMasterManifest(channelId) {
    if (sessions[channelId]) {
      const session = sessions[channelId];
      const masterM3U8 = await session.getMasterManifestAsync();
      return masterM3U8;
    } else {
      const err = new errs.NotFoundError('Invalid session');
      return Promise.reject(err)
    }
  }

  async getMediaManifests(channelId) {
    if (sessions[channelId]) {
      const allMediaM3U8 = {};
      let promises = [];
      const session = sessions[channelId];

      const bandwidths = this.options.channelManager
        .getChannels()
        .filter((ch) => ch.id === channelId)
        .pop()
        .profile
        .map((profile) => profile.bw);

      const addM3U8 = async (bw) => {
        allMediaM3U8[bw] = await session.getCurrentMediaManifestAsync(bw);
      }

      bandwidths.forEach((bw) => {
        promises.push(addM3U8(bw));
      })
      await Promise.all(promises);

      return allMediaM3U8;
    } else {
      const err = new errs.NotFoundError('Invalid session');
      return Promise.reject(err)
    }
  }

  async getAudioManifests(channelId) {
    if (sessions[channelId]) {
      const allAudioM3U8 = {};
      let promises = [];
      const session = sessions[channelId];
      const addM3U8 = async (groupId, lang) => {
        let audioM3U8 = await session.getCurrentAudioManifestAsync(groupId, lang);
        if (!allAudioM3U8[groupId]) {
          allAudioM3U8[groupId] = {};
        }
        allAudioM3U8[groupId][lang] = audioM3U8;
      }
      // Get m3u8s for all langauges for all groups
      const audioGroupsAndLangs = await session.getAudioGroupsAndLangs();
      for (const [audioGroup, languages] of Object.entries(audioGroupsAndLangs)) {
        (<Array<string>>languages).forEach((lang) => {
          promises.push(addM3U8(audioGroup, lang));
        });
      }
      await Promise.all(promises);

      return allAudioM3U8;
    } else {
      const err = new errs.NotFoundError('Invalid session');
      return Promise.reject(err)
    }
  }

  async getSubtitleManifests(channelId) {
    if (sessions[channelId]) {
      const allSubtitleM3U8 = {};
      let promises = [];
      const session = sessions[channelId];
      const addM3U8 = async (groupId, lang) => {
        let subtitleM3U8 = await session.getCurrentSubtitleManifestAsync(groupId, lang);
        if (!allSubtitleM3U8[groupId]) {
          allSubtitleM3U8[groupId] = {};
        }
        allSubtitleM3U8[groupId][lang] = subtitleM3U8;
      }
      // Get m3u8s for all langauges for all groups
      const subtitleGroupsAndLangs = await session.getSubtitleGroupsAndLangs();
      for (const [subtitleGroup, languages] of Object.entries(subtitleGroupsAndLangs)) {
        (<Array<string>>languages).forEach((lang) => {
          promises.push(addM3U8(subtitleGroup, lang));
        });
      }
      await Promise.all(promises);

      return allSubtitleM3U8;
    } else {
      const err = new errs.NotFoundError('Invalid session');
      return Promise.reject(err)
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
    let sessionLive;
    let options: any = {};
    if (req.query['playlist']) {
      // Backward compatibility
      options.category = req.query['playlist'];
    }
    if (req.query['category']) {
      options.category = req.query['category'];
    }
    if (this.autoCreateSession && req.query['channel']) {
      debug(`Attempting to create channel with id ${req.query['channel']}`);
      await this.createChannel(req.query['channel']);
      debug(`Automatically created channel with id ${req.query['channel']}`);
    }

    if (req.query['channel'] && sessions[req.query['channel']]) {
      session = sessions[req.query['channel']];
    } else if (req.query['session'] && sessions[req.query['session']]) {
      session = sessions[req.query['session']];
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
          "Content-Type": "application/vnd.apple.mpegurl",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "X-Session-Id",
          "Access-Control-Expose-Headers": "X-Session-Id",
          "Cache-Control": "max-age=300",
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
          "Content-Type": "application/vnd.apple.mpegurl",
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

  async _handleSubtitleManifest(req, res, next) {
    debug(`req.url=${req.url}`);
    const session = sessions[req.params[2]];
    if (session) {
      try {
        const body = await session.getCurrentSubtitleManifestAsync(
          req.params[0],
          req.params[1],
          req.headers["x-playback-session-id"]
        );
        res.sendRaw(200, Buffer.from(body, 'utf8'), {
          "Content-Type": "application/vnd.apple.mpegurl",
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

  async _handleDummySubtitleEndpoint(req,res,next) {
    debug(`req.url=${req.url}`);
    try {
      const body = `WEBVTT\nX-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000`;
      res.sendRaw(200, Buffer.from(body, 'utf8'), {
        "Content-Type": "text/vtt",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        "X-Instance-Id": this.instanceId + `<${version}>`,
      });
      next();
    } catch (err) {
      next(this._gracefulErrorHandler(err));
    }
  }

  async _handleSubtitleSliceEndpoint(req,res,next) {
    debug(`req.url=${req.url}`);
      try {
        const slicer = new SubtitleSlicer();
        const body = await slicer.generateVtt(req.query);
        res.sendRaw(200, Buffer.from(body, 'utf8'), {
          "Content-Type": "text/vtt",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
          "X-Instance-Id": this.instanceId + `<${version}>`,
        });
        next();
      } catch (err) {
        next(this._gracefulErrorHandler(err));
      }
  }

  async _handleMediaManifest(req, res, next) {
    debug(`x-playback-session-id=${req.headers["x-playback-session-id"]} req.url=${req.url}`);
    debug(req.params);
    const session = sessions[req.params[1]];
    const sessionLive = sessionsLive[req.params[1]];
    if (session && sessionLive) {
      try {
        let body = null;
        if (!this.streamSwitchManager) {
          debug(`[${req.params[1]}]: Responding with VOD2Live manifest`);
          body = await session.getCurrentMediaManifestAsync(req.params[0], req.headers["x-playback-session-id"]);
        } else {
          while (switcherStatus[req.params[1]] === null || switcherStatus[req.params[1]] === undefined) {
            debug(`[${req.params[1]}]: (${switcherStatus[req.params[1]]}) Waiting for streamSwitcher to respond`);
            await timer(500);
          }
          debug(`switcherStatus[${req.params[1]}]=[${switcherStatus[req.params[1]]}]`);
          if (switcherStatus[req.params[1]]) {
            debug(`[${req.params[1]}]: Responding with Live-stream manifest`);
            body = await sessionLive.getCurrentMediaManifestAsync(req.params[0]);
          } else {
            debug(`[${req.params[1]}]: Responding with VOD2Live manifest`);
            body = await session.getCurrentMediaManifestAsync(req.params[0], req.headers["x-playback-session-id"]);
          }
        }

        //verbose(`[${session.sessionId}] body=`);
        //verbose(body);
        res.sendRaw(200, Buffer.from(body, 'utf8'), {
          "Content-Type": "application/vnd.apple.mpegurl",
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
          playback: `/channels/${sessionId}/master.m3u8`,
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
      const sessionLive = sessionsLive[sessionId];
      if (session && sessionLive) {
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
