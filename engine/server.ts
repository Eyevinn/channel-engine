const fastify = require('fastify')();
const { v4: uuidv4 } = require('uuid');
const debug = require('debug')('engine-server');
const verbose = require('debug')('engine-server-verbose');
const path = require('path');
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
const preflight = require('./preflight.js');

const { version } = require('../package.json');

const AUTO_CREATE_CHANNEL_TIMEOUT = 3000;

const sessions = {}; // Should be a persistent store...
const sessionsLive = {}; // Should be a persistent store...
const sessionSwitchers = {}; // Should be a persistent store...
const switcherStatus = {}; // Should be a persistent store...
const eventStreams = {};
const DefaultDummySubtitleEndpointPath = "/dummyUrl"
const DefaultSubtitleSpliceEndpointPath = "/sliceUrl"

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
  vttBasePath?: string;
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
  sessionResetKey?: string;
  keepAliveTimeout?: number;
  sessionEventStream?: boolean;
  sessionHealthKey?: string;
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
  private sessionResetKey: string = "";
  private sessionEventStream: boolean = false;
  private sessionHealthKey: string = "";
  
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
    const vttBasePath = (options && options.vttBasePath) ? options.vttBasePath : '/vtt';
    this.dummySubtitleEndpoint = (options && options.dummySubtitleEndpoint) ? options.dummySubtitleEndpoint : vttBasePath + DefaultDummySubtitleEndpointPath;
    this.subtitleSliceEndpoint = (options && options.subtitleSliceEndpoint) ? options.subtitleSliceEndpoint : vttBasePath + DefaultSubtitleSpliceEndpointPath;

    this.sessionResetKey = "";
    if (options && options.sessionResetKey) {
      this.sessionResetKey = options.sessionResetKey;
    }
    if (options && options.sessionHealthKey) {
      this.sessionHealthKey = options.sessionHealthKey;
    }
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
    this.server = fastify;
    if (options && options.keepAliveTimeout) {
      this.server.server.keepAliveTimeout = options.keepAliveTimeout;
      this.server.server.headersTimeout = options.keepAliveTimeout + 1000;  
    }
    // this.server.register(cors, {
    //   origin: '*',
    //   methods: ['GET', 'POST', 'PUT', 'DELETE'], 
    // });
    this.server.options('*', preflight.handler); 

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
      this.server.register(require('fastify-static'), {
        root: path.join(__dirname, options.staticDirectory),
        prefix: '/', 
      });
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
    const handleMasterRoute = async (req, res) => {
      debug(req.params);
      let m;
      if (req.params.file.match(/master.m3u8/)) {
        await this._handleMasterManifest(req, res);
      } else if (m = req.params.file.match(/master(\d+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        await this._handleMediaManifest(req, res);
      } else if (m = req.params.file.match(/master-(\S+)_(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        req.params[2] = m[3];
        await this._handleAudioManifest(req, res);
      } else if (m = req.params.file.match(/subtitles-(\S+)_(\S+).m3u8;session=(.*)$/)) {
        req.params[0] = m[1];
        req.params[1] = m[2];
        req.params[2] = m[3];
        await this._handleSubtitleManifest(req, res);
      }
    };
    this.server.get('/live/:file', async (req, res) => {
      await handleMasterRoute(req, res);
    });
    this.server.get('/channels/:channelId/:file', async (req, res) => {
      req.query['channel'] = req.params.channelId;
      await handleMasterRoute(req, res);
    });

    this.server.get('/eventstream/:sessionId', this._handleEventStream.bind(this));
    this.server.get('/status/:sessionId', this._handleStatus.bind(this));
    this.server.get('/health', this._handleAggregatedSessionHealth.bind(this));
    this.server.get('/health/:sessionId', this._handleSessionHealth.bind(this));
    this.server.get('/reset', this._handleSessionsReset.bind(this));
    this.server.get('/reset/:sessionId', this._handleSessionReset.bind(this));
    this.server.get(vttBasePath + DefaultDummySubtitleEndpointPath, this._handleDummySubtitleEndpoint.bind(this));
    this.server.get(vttBasePath + DefaultSubtitleSpliceEndpointPath, this._handleSubtitleSliceEndpoint.bind(this));

    this.server.setNotFoundHandler((request, reply) => {
      reply.header("X-Instance-Id", this.instanceId + `<${version}>`);
      reply.status(404).send({ message: "Not Found" });
    });
    
    this.server.setErrorHandler((error, request, reply) => {
      reply.header("X-Instance-Id", this.instanceId + `<${version}>`);
      reply.status(500).send({ message: "Internal Server Error", error: error.message });
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
        sessionResetKey: options.sessionResetKey,
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
        sessionEventStream: options.sessionEventStream
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
    this.server.listen({ port: port}, (err) => {
      if (err) {
        console.error(err);
        process.exit(1);
      }
    });
    debug('%s listening at %s', this.server.name, this.server.url);
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
      return Promise.reject({ message: 'Invalid session' })
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
      return Promise.reject({ message: 'Invalid session' })
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
      return Promise.reject({ message: 'Invalid session' })
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
      return Promise.reject({ message: 'Invalid session' })
    }
  }

  _handleHeartbeat(request, reply) {
    debug('req.url=' + request.url);
    reply.status(200).send();
  }

  async _handleMasterManifest(request, reply) {
    debug('req.url=' + request.url);
    debug(request.query);
    let session;
    let options: any = {};
    
    if (request.query['playlist']) {
      options.category = request.query['playlist'];
    }
    if (request.query['category']) {
      options.category = request.query['category'];
    }
    if (this.autoCreateSession && request.query['channel']) {
      debug(`Attempting to create channel with id ${request.query['channel']}`);
      await this.createChannel(request.query['channel']);
      debug(`Automatically created channel with id ${request.query['channel']}`);
    }
  
    if (request.query['channel'] && sessions[request.query['channel']]) {
      session = sessions[request.query['channel']];
    } else if (request.query['session'] && sessions[request.query['session']]) {
      session = sessions[request.query['session']];
    }
    if (request.query['startWithId']) {
      options.startWithId = request.query['startWithId'];
      debug(`New session to start with assetId=${options.startWithId}`);
    }
  
    if (session) {
      const eventStream = new EventStream(session);
      eventStreams[session.sessionId] = eventStream;
  
      let filter;
      if (request.query['filter']) {
        debug(`Applying filter on master manifest ${request.query['filter']}`);
        filter = filterQueryParser(request.query['filter']);
      }
  
      try {
        const body = await session.getMasterManifestAsync(filter);
        reply.raw.writeHead(200, {
          "Content-Type": "application/vnd.apple.mpegurl",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "X-Session-Id",
          "Access-Control-Expose-Headers": "X-Session-Id",
          "Cache-Control": "max-age=300",
          "X-Session-Id": session.sessionId,
          "X-Instance-Id": this.instanceId + `<${version}>`,
        });
        reply.raw.end(Buffer.from(body, 'utf8'));
      } catch (err) {
        throw this._errorHandler(err);
      }
    } else {
      throw this._gracefulErrorHandler("Could not find a valid session");
    }
  }

  async _handleAudioManifest(request, reply) {
    debug(`req.url=${request.url}`);
    const session = sessions[request.params[2]];
    if (session) {
      try {
        const body = await session.getCurrentAudioManifestAsync(
          request.params[0],
          request.params[1],
          request.headers["x-playback-session-id"]
        );
        reply.raw.writeHead(200, {
          "Content-Type": "application/vnd.apple.mpegurl",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
          "X-Instance-Id": this.instanceId + `<${version}>`,
        });
        reply.raw.end(Buffer.from(body, 'utf8'));
      } catch (err) {
        throw this._gracefulErrorHandler(err);
      }
    } else {
      reply.status(404).send({ message: 'Invalid session' });
      return;
    }
  }

  async _handleSubtitleManifest(request, reply) {
    debug(`req.url=${request.url}`);
    const session = sessions[request.params[2]];
    if (session) {
      try {
        const body = await session.getCurrentSubtitleManifestAsync(
          request.params[0],
          request.params[1],
          request.headers["x-playback-session-id"]
        );
        reply.raw.writeHead(200, {
          "Content-Type": "application/vnd.apple.mpegurl",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
          "X-Instance-Id": this.instanceId + `<${version}>`,
        });
        reply.raw.end(Buffer.from(body, 'utf8'));
      } catch (err) {
        throw this._gracefulErrorHandler(err);
      }
    } else {
      reply.status(404).send({ message: 'Invalid session' });
      return;
    }
  }

  async _handleDummySubtitleEndpoint(request, reply) {
    debug(`req.url=${request.url}`);
    try {
      const body = `WEBVTT\nX-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n\n`;
      reply.raw.writeHead(200, {
        "Content-Type": "text/vtt",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        "X-Instance-Id": this.instanceId + `<${version}>`,
      });
      reply.raw.end(Buffer.from(body, 'utf8'));
    } catch (err) {
      throw this._gracefulErrorHandler(err);
    }
  }
  
  async _handleSubtitleSliceEndpoint(request, reply) {
    debug(`req.url=${request.url}`);
    try {
      const slicer = new SubtitleSlicer();
      const body = await slicer.generateVtt(request.query);
      reply.raw.writeHead(200, {
        "Content-Type": "text/vtt",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        "X-Instance-Id": this.instanceId + `<${version}>`,
      });
      reply.raw.end(Buffer.from(body, 'utf8'));
    } catch (err) {
      throw this._gracefulErrorHandler(err);
    }
  }

  async _handleMediaManifest(request, reply) {
  debug(`x-playback-session-id=${request.headers["x-playback-session-id"]} req.url=${request.url}`);
  debug(request.params);
  const session = sessions[request.params[1]];
  const sessionLive = sessionsLive[request.params[1]];

  if (session && sessionLive) {
    try {
      let body = null;
      if (!this.streamSwitchManager) {
        debug(`[${request.params[1]}]: Responding with VOD2Live manifest`);
        body = await session.getCurrentMediaManifestAsync(request.params[0], request.headers["x-playback-session-id"]);
      } else {
        while (switcherStatus[request.params[1]] === null || switcherStatus[request.params[1]] === undefined) {
          debug(`[${request.params[1]}]: (${switcherStatus[request.params[1]]}) Waiting for streamSwitcher to respond`);
          await timer(500);
        }
        debug(`switcherStatus[${request.params[1]}]=[${switcherStatus[request.params[1]]}]`);
        if (switcherStatus[request.params[1]]) {
          debug(`[${request.params[1]}]: Responding with Live-stream manifest`);
          body = await sessionLive.getCurrentMediaManifestAsync(request.params[0]);
        } else {
          debug(`[${request.params[1]}]: Responding with VOD2Live manifest`);
          body = await session.getCurrentMediaManifestAsync(request.params[0], request.headers["x-playback-session-id"]);
        }
      }

      reply.raw.writeHead(200, {
        "Content-Type": "application/vnd.apple.mpegurl",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": `max-age=${this.streamerOpts.cacheTTL || '4'}`,
        "X-Instance-Id": this.instanceId + `<${version}>`,
      });
      reply.raw.end(Buffer.from(body, 'utf8'));
    } catch (err) {
      throw this._gracefulErrorHandler(err);
    }
  } else {
    reply.status(404).send({ message: 'Invalid session' });
    return;
  }
  }

  async _handleEventStream(request, reply) {
  debug(`req.url=${request.url}`);
  const eventStream = eventStreams[request.params.sessionId];

  if (eventStream) {
    try {
      const body = await eventStream.poll();
      reply.status(200).send(body); 
    } catch (err) {
      throw this._errorHandler(err);
    }
  } else {
    debug(`No event stream found for session=${request.params.sessionId}`);
    reply.status(200).send({});
  }
  }

  async _handleStatus(request, reply) {
    debug(`req.url=${request.url}`);
    const session = sessions[request.params.sessionId];
    
    if (session) {
      const body = await session.getStatusAsync();
      reply.status(200).send(body);
    } else {
      reply.status(404).send({ message: 'Invalid session' });
      return;
    }
  }

  async _handleAggregatedSessionHealth(request, reply) {
    debug(`req.url=${request.url}`);
    
    if (this.sessionHealthKey && this.sessionHealthKey !== request.headers['x-health-key']) {
      reply.status(403).send(JSON.stringify({ "message": "Invalid Session-Health-Key" }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
      return;
    }
  
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
      reply.status(200).send(JSON.stringify({ 
        "health": "ok", 
        "engine": engineStatus, 
        "count": endpoints.length, 
        "sessionEndpoints": endpoints 
      }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",  
      });
    } else {
      reply.status(503).send(JSON.stringify({ 
        "health": "unhealthy", 
        "engine": engineStatus, 
        "failed": failingSessions 
      }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
    }
  }

  async _handleSessionHealth(request, reply) {
    debug(`req.url=${request.url}`);
    const session = sessions[request.params.sessionId];
    
    if (session) {
      const status = await session.getStatusAsync();
      
      if (status.playhead && status.playhead.state === "running") {
        reply.status(200).send(JSON.stringify({ 
          "health": "ok", 
          "tick": status.playhead.tickMs, 
          "mediaSeq": status.playhead.mediaSeq 
        }), {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",  
        });
      } else {
        reply.status(503).send(JSON.stringify({ 
          "health": "unhealthy" 
        }), {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",
        });
      }
    } else {
      reply.status(404).send({ message: 'Invalid session' });
      return;
    }
  }

  async _handleSessionsReset(request, reply) {
    debug(`req.url=${request.url}`);
    
    if (this.sessionResetKey && request.query.key !== this.sessionResetKey) {
      reply.status(403).send(JSON.stringify({ "message": "Invalid Session-Reset-Key" }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
      return;
    }
  
    let sessionResets = [];
    
    for (const sessionId of Object.keys(sessions)) {
      const session = sessions[sessionId];
      const sessionLive = sessionsLive[sessionId];
      
      if (session && sessionLive) {
        await session.resetAsync(); 
        sessionResets.push(sessionId);
      } else {
        reply.status(404).send({ message: 'Invalid session' });
        return;
      }
    }
  
    reply.status(200).send(JSON.stringify({ 
      "status": "ok", 
      "instanceId": this.instanceId, 
      "resets": sessionResets 
    }), {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-cache",
    });
  }

  async _handleSessionReset(request, reply) {
    debug(`req.url=${request.url}`);

    if (this.sessionResetKey && request.query.key !== this.sessionResetKey) {
      reply.status(403).send(JSON.stringify({ "message": "Invalid Session-Reset-Key" }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
      return;
    }

    try {
      let sessionId;
      if (request.params && request.params.sessionId) {
        sessionId = request.params.sessionId;
      }

      let sessionResets = [];
      const session = sessions[sessionId];
      const sessionLive = sessionsLive[sessionId];

      if (session && sessionLive) {
        await session.resetAsync(sessionId);
        sessionResets.push(sessionId);
      } else {
        reply.status(400).send(JSON.stringify({ "message": "Invalid Session ID" }), {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache",
        });
        return;
      }

      reply.status(200).send(JSON.stringify({
        "status": "ok",
        "instanceId": this.instanceId,
        "resets": sessionResets
      }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
    } catch (e) {
      reply.status(500).send(JSON.stringify({ "error": e }), {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
      });
      reply.status(404).send({ message: 'Invalid session' });
      return;
    }
  }


  _gracefulErrorHandler(errMsg) {
    console.error(errMsg);
    const err = new Error(errMsg);
    return err;
  }

  _errorHandler(errMsg) {
    console.error(errMsg);
    const err = new Error(errMsg);
    return err;
  }
}
