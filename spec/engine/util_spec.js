const { filterQueryParser } = require('../../engine/util.js');

fdescribe("Filter Query parser", () => {
  it("can handle systemBitrate low/high range", () => {
    const filterQuery = `(type=="video"&&systemBitrate>100000)&&(type=="video"&&systemBitrate<800000)`;
    const filter = filterQueryParser(filterQuery);
  
    expect(filter.video.systemBitrate.low).toEqual(100000);
    expect(filter.video.systemBitrate.high).toEqual(800000);  
  });
});