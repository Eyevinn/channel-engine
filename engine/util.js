const { version } = require('../package.json');

const filterQueryParser = (filterQuery) => {
  const conditions = filterQuery.match(/\(([^\(\)]*?)\)/g);

  let filter = {};
  conditions.map((c) => {
    const m = c.match(/\(type=="(.*?)"(&&|\|\|)(.*?)(<|>)(.*)\)/);
    if (m) {
      const type = m[1];
      const operator = m[2];
      const key = m[3];
      const comp = m[4];
      const value = m[5];
      
      if (!filter[type]) {
        filter[type] = {};
      }
      if (operator === "&&") {
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
  return profiles.filter(profile => {
    if (filter.video && filter.video.systemBitrate) {
      return (profile.bw >= filter.video.systemBitrate.low && 
        profile.bw <= filter.video.systemBitrate.high);
    } else if (filter.video && filter.video.height) {
      return (profile.resolution[1] >= filter.video.height.low &&
        profile.resolution[1] <= filter.video.height.high);
    }
    return true;
  });
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

module.exports = {
  filterQueryParser,
  applyFilter,
  cloudWatchLog,
  m3u8Header,
  toHHMMSS,
}