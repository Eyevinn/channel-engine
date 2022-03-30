const { filterQueryParser, applyFilter } = require('../../engine/util.js');

describe("Filter Query parser", () => {
  it("can handle systemBitrate low/high range", () => {
    const filterQuery = `(type=="video"ANDsystemBitrate>100000)AND(type=="video"ANDsystemBitrate<800000)`;
    const filter = filterQueryParser(filterQuery);
  
    expect(filter.video.systemBitrate.low).toEqual(100000);
    expect(filter.video.systemBitrate.high).toEqual(800000);  
  });

  it("can handle resolution low/high range", () => {
    const filterQuery = `(type=="video"ANDheight>600)AND(type=="video"ANDheight<1080)`;
    const filter = filterQueryParser(filterQuery);

    expect(filter.video.height.low).toEqual(600);
    expect(filter.video.height.high).toEqual(1080);  
  });

  it("can handle a faulty filter query", () => {
    const filterQuery = `type=video&height=600&type=video&height=1080`;
    const filter = filterQueryParser(filterQuery);

    expect(filter).toEqual({}); 
  });
});

describe("Profile filter", () => {
  let PROFILE;

  beforeEach(() => {
    PROFILE = [
      {
        bw: 6134000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 1024, 458 ]
      },
      {
        bw: 2323000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 640, 286 ]
      },
      {
        bw: 1313000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 480, 214 ]
      }
    ],
    [
      {
        bw: 6134000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 1024, 458 ]
      },
      {
        bw: 2323000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 640, 286 ]
      },
      {
        bw: 1313000,
        codecs: 'avc1.4d001f,mp4a.40.2',
        resolution: [ 480, 214 ]
      }
    ];
  });

  
  it ("can filter out video tracks with height min 200 and max 400", () => {
    const filterQuery = `(type=="video"ANDheight>200)AND(type=="video"ANDheight<400)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(2);
  });

  it ("can filter out video tracks with bitrate min 1414000 and max 3232000", () => {
    const filterQuery = `(type=="video"ANDsystemBitrate>1414000)AND(type=="video"ANDsystemBitrate<3232000)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(1);
    expect(filteredProfiles[0].bw).toEqual(2323000);
  });

  it ("can filter out video tracks with bitrate min 1414000 and height min 300", () => {
    const filterQuery = `(type=="video"ANDsystemBitrate>1414000)AND(type=="video"ANDheight>300)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(1);
    expect(filteredProfiles[0].bw).toEqual(6134000);
  });

  it ("can filter out video tracks with bitrate min 1414000", () => {
    const filterQuery = `(type=="video"ANDsystemBitrate>1414000)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(2);
    expect(filteredProfiles[1].bw).toEqual(2323000);
    expect(filteredProfiles[0].bw).toEqual(6134000);
  });

  it ("can filter out video tracks with bitrate max 1414000", () => {
    const filterQuery = `(type=="video"ANDsystemBitrate<1414000)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(1);
    expect(filteredProfiles[0].bw).toEqual(1313000);
  });

  it ("can filter out video tracks with height min 250", () => {
    const filterQuery = `(type=="video"ANDheight>250)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(2);
    expect(filteredProfiles[1].bw).toEqual(2323000);
    expect(filteredProfiles[0].bw).toEqual(6134000);
  });

  it ("can filter out video tracks with height max 300", () => {
    const filterQuery = `(type=="video"ANDheight<300)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(2);
    expect(filteredProfiles[1].bw).toEqual(1313000);
    expect(filteredProfiles[0].bw).toEqual(2323000);
  });

});