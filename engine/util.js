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

module.exports = {
  filterQueryParser,
  applyFilter
}