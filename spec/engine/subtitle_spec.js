const SubtitleSlicer = require('../../engine/subtitle_slicer.js');
const fs = require("fs");



describe("Subtitle slicer", () => {
  let mockWebVtt;
  let mockWebVtt2;
  beforeEach(() => {
    mockWebVtt = fs.createReadStream("spec/testvectors/subtitle_file.webvtt");
    mockWebVtt2 = fs.createReadStream("spec/testvectors/subtitle_file_2.webvtt");
  });

  it("generate sliced vtt with correct time stamps", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 15);
    params.append("endtime", 20);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[0]).toEqual("WEBVTT");
    expect(subStrings[1]).toEqual("X-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000");
    expect(subStrings[3]).toEqual("00:00:15.000 --> 00:00:18.000");
    expect(subStrings[4]).toEqual("À votre gauche vous pouvez voir...");
    expect(subStrings[6]).toEqual("00:00:18.000 --> 00:00:20.000");
    expect(subStrings[7]).toEqual("À votre droite vous pouvez voir les...");
    done();
  });

  it("generate sliced with correct line without comment", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 26);
    params.append("endtime", 27);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[2]).toEqual("");
    expect(subStrings[3]).toEqual("00:00:26.000 --> 00:00:27.000");
    expect(subStrings[4]).toEqual("Emo ?");
    done();
  });

  it("with explicit cue positions", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 51);
    params.append("endtime", 53);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[3]).toEqual("00:00:51.000 --> 00:00:53.000 align:left size:50%");
    expect(subStrings[4]).toEqual("Je crois pas...|et vous ?");
    done();
  });

  it("generate sliced with all lines relevant to timestamp", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 55);
    params.append("endtime", 57);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[3]).toEqual("00:00:55.000 --> 00:00:57.000");
    expect(subStrings[4]).toEqual("Ça va.");
    expect(subStrings[5]).toEqual("Ça va.");
    expect(subStrings[6]).toEqual("Ça va.");
    done();
  });

  it("with chapters", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 62);
    params.append("endtime", 63);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[3]).toEqual("Slide 3");
    expect(subStrings[4]).toEqual("00:01:02.000 --> 00:01:03.000");
    expect(subStrings[5]).toEqual("Allons-y.");
    done();
  });

  it("with text that is longer than one segment", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    params.append("starttime", 64);
    params.append("endtime", 68);
    params.append("elapsedtime", 0);
    vttFile = await subslice.generateVtt(params, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[3]).toEqual("00:01:03.000 --> 00:01:09.000");
    expect(subStrings[4]).toEqual("Et après ?");
    done();
  });

  it("that handles slice over multiple files", async (done) => {
    let subslice = new SubtitleSlicer();
    let params = new URLSearchParams();
    let perviousParams = new URLSearchParams();
    perviousParams.append("starttime", 63)
    perviousParams.append("endtime", 69)
    perviousParams.append("elapsedtime", 0)
    params.append("previousvtturi", perviousParams)
    params.append("starttime", 0)
    params.append("endtime", 3)
    params.append("elapsedtime", 69)
    vttFile = await subslice.generateVtt(params, mockWebVtt2, mockWebVtt)
    const subStrings = vttFile.split("\n")
    expect(subStrings[3]).toEqual("00:01:03.000 --> 00:01:09.000");
    expect(subStrings[4]).toEqual("Et après ?");
    expect(subStrings[6]).toEqual("00:01:09.000 --> 00:01:12.000");
    expect(subStrings[7]).toEqual("À votre gauche vous pouvez voir...");
    done();
  });
});
