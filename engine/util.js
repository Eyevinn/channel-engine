const { version } = require('../package.json');

const filterQueryParser = (filterQuery) => {
  const conditions = filterQuery.match(/\(([^\(\)]*?)\)/g);

  let filter = {};
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
    logEntry.time = (new Date()).toISOString();
    console.log(JSON.stringify(logEntry));
  }
};

const m3u8Header = (instanceId) => {
  let m3u8 = "";
  m3u8 += `## Created with Eyevinn Channel Engine library (version=${version}${instanceId ? "<" + instanceId + ">" : "" })\n`;
  m3u8 += "##    https://www.npmjs.com/package/eyevinn-channel-engine\n";
  return m3u8;
};

const toHHMMSS = (secs) => {
  var sec_num = parseInt(secs, 10)
  var hours   = Math.floor(sec_num / 3600)
  var minutes = Math.floor(sec_num / 60) % 60
  var seconds = sec_num % 60

  return [hours,minutes,seconds]
      .map(v => v < 10 ? "0" + v : v)
      .join(":")
};

const logerror = (sessionId, err) => {
  console.error(`ERROR [${sessionId}]:`);
  console.error(err);
};

module.exports = {
  filterQueryParser,
  applyFilter,
  cloudWatchLog,
  m3u8Header,
  toHHMMSS,
  logerror
}