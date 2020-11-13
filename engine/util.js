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

module.exports = {
  filterQueryParser
}