const { version } = require("../package.json");
const fetch = require("node-fetch");
const { AbortController } = require("abort-controller");

const filterQueryParser = (filterQuery) => {
  const conditions = filterQuery.match(/\(([^\(\)]*?)\)/g);

  let filter = {};
  
  if (!conditions) {
    return filter;
  }
  conditions.map((c) => {
    const m = c.match(/\(type=="(.*?)"(AND|\|\|)(.*?)(<|>)(.*)\)/);
    if (m) {
      const type = m[1];
      const operator = m[2];
      const key = m[3];
      const comp = m[4];
      const value = m[5];

      if (!filter[type]) {
        filter[type] = {};
      }
      if (operator === "AND") {
        if (!filter[type][key]) {
          filter[type][key] = {};
        }
        if (comp === "<") {
          filter[type][key].high = parseInt(value, 10);
        } else if (comp === ">") {
          filter[type][key].low = parseInt(value, 10);
        }
      }
    }
  });
  return filter;
};

const applyFilter = (profiles, filter) => {
  let filteredProfiles = {};
  const supportedFilterKeys = ["systemBitrate", "height"];

  if (!filter.video) {
    return profiles;
  }

  const keys = Object.keys(filter.video);
  if (supportedFilterKeys.every((supportedKey) => !keys.includes(supportedKey))) {
    return profiles;
  }

  if (filter.video.systemBitrate) {
    filteredProfiles = profiles.filter((profile) => {
      if (filter.video.systemBitrate.low && filter.video.systemBitrate.high) {
        return profile.bw >= filter.video.systemBitrate.low && profile.bw <= filter.video.systemBitrate.high;
      } else if (filter.video.systemBitrate.low && !filter.video.systemBitrate.high) {
        return profile.bw >= filter.video.systemBitrate.low;
      } else if (!filter.video.systemBitrate.low && filter.video.systemBitrate.high) {
        return profile.bw <= filter.video.systemBitrate.high;
      }
      return true;
    });
  }

  if (filter.video.height) {
    let toFilter = profiles;
    if (!ItemIsEmpty(filteredProfiles)) {
      toFilter = filteredProfiles;
    }
    filteredProfiles = toFilter.filter((profile) => {
      if (filter.video.height.low && filter.video.height.high) {
        return profile.resolution[1] >= filter.video.height.low && profile.resolution[1] <= filter.video.height.high;
      } else if (filter.video.height.low) {
        return profile.resolution[1] >= filter.video.height.low;
      } else if (filter.video.height.high) {
        return profile.resolution[1] <= filter.video.height.high;
      }
      return true;
    });
  }

  return filteredProfiles;
};

const ItemIsEmpty = (obj) => {
  if (!obj) {
    return true;
  }
  for (var key in obj) {
    if (obj.hasOwnProperty(key)) {
      return false;
    }
  }
  return true;
};

const cloudWatchLog = (silent, type, logEntry) => {
  if (!silent) {
    logEntry.type = type;
    logEntry.time = new Date().toISOString();
    console.log(JSON.stringify(logEntry));
  }
};

const m3u8Header = (instanceId) => {
  let m3u8 = "";
  m3u8 += `## Created with Eyevinn Channel Engine library (version=${version}${instanceId ? "<" + instanceId + ">" : ""})\n`;
  m3u8 += "##    https://www.npmjs.com/package/eyevinn-channel-engine\n";
  return m3u8;
};

const toHHMMSS = (secs) => {
  var sec_num = parseInt(secs, 10);
  var hours = Math.floor(sec_num / 3600);
  var minutes = Math.floor(sec_num / 60) % 60;
  var seconds = sec_num % 60;

  return [hours, minutes, seconds].map((v) => (v < 10 ? "0" + v : v)).join(":");
};

const logerror = (sessionId, err) => {
  console.error(`ERROR [${sessionId}]:`);
  console.error(err);
};

const timer = (ms) => new Promise((res) => setTimeout(res, ms));

class WaitTimeGenerator {
  constructor(defaultIntervalMs, minValue) {
    (this.timestamp = null), (this.prevWaitTime = null), (this.defaultIntervalMs = defaultIntervalMs || 3000), (this.minValue = minValue);
  }
  _getWaitTimeFromTimestamp() {
    if (!this.timestamp) {
      this.timestamp = new Date();
    }
    const sec = this.timestamp.getSeconds();
    const defaultSec = this.defaultIntervalMs / 1000;
    const d = parseInt(sec / defaultSec);
    const waitSec = defaultSec * (d + 1) - sec;
    return waitSec * 1000;
  }
  async getWaitTime(plannedTime) {
    if (!this.timestamp || (this.prevWaitTime === this.minValue && plannedTime !== this.minValue)) {
      this.timestamp = new Date();
      const waitMs = this._getWaitTimeFromTimestamp();
      this.prevWaitTime = waitMs;
      return waitMs;
    }
    this.prevWaitTime = plannedTime;
    return plannedTime;
  }
}

const findNearestValue = (val, array) => {
  const sorted = array.sort((a, b) => b - a);
  return sorted.reduce((a, b) => {
    return Math.abs(b - val) < Math.abs(a - val) ? b : a;
  });
}

const isValidUrl = (url) => {
  try {
    const _url = new URL(url);
    return true;
  } catch (err) {
    return false;
  }
}


const fetchWithRetry = async (uri, opts, maxRetries, retryDelayMs, timeoutMs, debug) => {
  if (!debug) {
    var debug = (msg) => { console.log(msg); };
  } 
  let tryFetchCount = 0;
  const RETRY_LIMIT = maxRetries || 5;
  const TIMEOUT_LIMIT = timeoutMs || 10 * 1000;
  const RETRY_DELAY = retryDelayMs || 1000;
  while (tryFetchCount < RETRY_LIMIT) {
    tryFetchCount++;
    debug(`Fetching URI -> ${uri}, attempt ${tryFetchCount} of ${maxRetries}`);
    //
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      `Request Timeout after (${TIMEOUT_LIMIT})ms @ ${uri}`;
      controller.abort();
    }, TIMEOUT_LIMIT);
    try {
      const fetchOpts = Object.assign({ signal: controller.signal }, opts);
      const response = await fetch(uri, fetchOpts);

      if (response.status >= 400 && response.status < 600) {
        const msg = `Bad response from URI: ${uri}\nReturned Status Code: ${response.status}`;
        debug(msg);
        if (tryFetchCount === maxRetries) {
          return Promise.resolve(response);
        }
        debug(`Going to try fetch again in ${RETRY_DELAY}ms`);
        await timer(RETRY_DELAY);
        continue;
      }
      // Return Good response
      return Promise.resolve(response);
    } catch (err) {
      debug(`Node-Fetch Error on URI: ${uri}\nFull Error -> ${err}`);
      if (tryFetchCount === maxRetries) {
        return Promise.reject(err);
      }
      continue;
    } finally {
      clearTimeout(timeout);
    }
  }
};

const codecsFromString = (codecs) => {
  const audioCodecs = codecs.split(",").find(c => {
    return c.match(/^mp4a/) || c.match(/^ac-3/) || c.match(/^ec-3/);
  });
  const videoCodecs = codecs.split(",").find(c => {
    return c.match(/^avc/) || c.match(/^hevc/) || c.match(/^dvh/);
  });
  return [videoCodecs, audioCodecs];
};

module.exports = {
  filterQueryParser,
  applyFilter,
  cloudWatchLog,
  m3u8Header,
  toHHMMSS,
  logerror,
  timer,
  WaitTimeGenerator,
  findNearestValue,
  isValidUrl,
  fetchWithRetry,
  codecsFromString
};
