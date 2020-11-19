const { filterQueryParser, applyFilter } = require('../../engine/util.js');

describe("Filter Query parser", () => {
  it("can handle systemBitrate low/high range", () => {
    const filterQuery = `(type=="video"&&systemBitrate>100000)&&(type=="video"&&systemBitrate<800000)`;
    const filter = filterQueryParser(filterQuery);
  
    expect(filter.video.systemBitrate.low).toEqual(100000);
    expect(filter.video.systemBitrate.high).toEqual(800000);  
  });

  it("can handle resolution low/high range", () => {
    const filterQuery = `(type=="video"&&height>600)&&(type=="video"&&height<1080)`;
    const filter = filterQueryParser(filterQuery);

    expect(filter.video.height.low).toEqual(600);
    expect(filter.video.height.high).toEqual(1080);  
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
    const filterQuery = `(type=="video"&&height>200)&&(type=="video"&&height<400)`;
    const filter = filterQueryParser(filterQuery);

    const filteredProfiles = applyFilter(PROFILE, filter);
    expect(filteredProfiles.length).toEqual(2);
  });
});