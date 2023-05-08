const fetch = require("node-fetch");
const fs = require("fs");
class SubtitleSlicer {
  constructor() {
    this.vttFiles = {};
  }

  async getVttFile(url) {
    let resp = await fetch(url)
    if (resp.status === 200) { // TODO add error handeling
      let text = await resp.text();
      return text;
    }
    else {
      return "";
    }

  }

  checkTimeStamp(line, startTime, endTime, elapsedtime) {
    const times = line.split("-->");
    let startTimeTimestamp = times[0].split(":");
    let endTimeTimestamp = times[1].split(":");
    let startTimeTimestampInSec = parseInt(startTimeTimestamp[0]) * 3600;
    startTimeTimestampInSec += parseInt(startTimeTimestamp[1]) * 60;
    const startTimeSecondsAndFractions = startTimeTimestamp[2].split(".");
    startTimeTimestampInSec += parseInt(startTimeSecondsAndFractions[0]);

    let endTimeTimestampInSec = parseInt(endTimeTimestamp[0]) * 3600;
    endTimeTimestampInSec += parseInt(endTimeTimestamp[1]) * 60;
    const endTimeSecondsAndFractions = endTimeTimestamp[2].split(".");
    endTimeTimestampInSec += parseInt(endTimeSecondsAndFractions[0]);
    startTime = parseInt(startTime);
    endTime = parseInt(endTime);
    elapsedtime = parseInt(elapsedtime);
    startTime += elapsedtime;
    endTime += elapsedtime;

    if (startTime <= startTimeTimestampInSec && startTimeTimestampInSec < endTime) {
      return true;
    }
    if (startTime < endTimeTimestampInSec && endTimeTimestampInSec <= endTime) {
      return true;
    }
    if (startTimeTimestampInSec < startTime && endTime < endTimeTimestampInSec) {
      return true;
    }

    return false

  }

  streamToString(stream) {
    const chunks = [];
    return new Promise((resolve, reject) => {
      stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      stream.on('error', (err) => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    })
  }

  async generateVtt(params, _injectedVttFile, _injectedPreviousVttFile) {
    const paramEncode = new URLSearchParams(params)
    const uri = paramEncode.get("vtturi");
    const startTime = paramEncode.get("starttime");
    const endTime = paramEncode.get("endtime");
    const elapsedTime = paramEncode.get("elapsedtime");
    const previousParams = new URLSearchParams(paramEncode.get("previousvtturi"))
    const previousUri = previousParams.get("vtturi");
    const previousStartTime = previousParams.get("starttime");
    const previousEndTime = previousParams.get("endtime");
    const previousElapsedTime = previousParams.get("elapsedtime");

    let file = "";
    let previousFile = "";
    let newFile = "";
    if (uri) {
      if (this.vttFiles.length) {
        file = this.vttFile[uri];
      }
      if (!file) {
        file = await this.getVttFile(uri)
      }
    } else if (_injectedVttFile) {
      file = await this.streamToString(_injectedVttFile)
    } else {
      console.error("no vtt file provided");
    }

    if (previousUri) {
      if (this.vttFiles.length) {
        previousFile = this.vttFile[previousUri];
      }
      if (!previousFile) {
        previousFile = await this.getVttFile(previousUri)
      }
    } else if (_injectedPreviousVttFile) {
      previousFile = await this.streamToString(_injectedPreviousVttFile)
    }

    const previousFileLines = previousFile.split("\n");
    let previousFileContentToAdd = "";
    for (let i = 0; i < previousFileLines.length; i++) {
      const ss = previousFileLines[i];
      if (ss.match(/(\d+):(\d+):(\d+).(\d+) --> (\d+):(\d+):(\d+).(\d+)/)?.input) {
        let shouldAdd = this.checkTimeStamp(ss, previousStartTime, previousEndTime, previousElapsedTime)
        if (shouldAdd) {
          if (previousFileLines[i - 1]) {
            if (previousFileLines[i - 1].slice(0, 4) !== "NOTE")
            previousFileContentToAdd += previousFileLines[i - 1] + "\n";
          }

          previousFileContentToAdd += ss + "\n";
          
          let j = 1;
          while (previousFileLines.length > i + j) {
            if (previousFileLines[i + j]) {
              previousFileContentToAdd += previousFileLines[i + j] + "\n";
            } else {
              break;
            }
            j++;
          }
          previousFileContentToAdd += "\n"
        }
      }
    }

    const lines = file.split("\n")
    let addedOnce = false;

    for (let i = 0; i < lines.length; i++) {
      let ss = lines[i]
      switch (ss) {
        case ss.match("WEBVTT")?.input:
          newFile += ss + "\n"
          break;
        case ss.match(/X-TIMESTAMP-MAP/)?.input:
          if (!ss.match(/LOCAL:00:00:00.000/) || !ss.match(/MPEGTS:0/)) {
            console.warn("MPEGTS and/or LOCAL is not zero")
          }
          newFile += ss + "\n\n"
          break;
        case ss.match(/(\d+):(\d+):(\d+).(\d+) --> (\d+):(\d+):(\d+).(\d+)/)?.input:
          let shouldAdd = this.checkTimeStamp(ss, startTime, endTime, elapsedTime)
          if (shouldAdd) {
            if (!addedOnce) {
              addedOnce = true;
              newFile += previousFileContentToAdd;
            }

            if (lines[i - 1]) {
              if (lines[i - 1].slice(0, 4) !== "NOTE")
                newFile += lines[i - 1] + "\n";
            }
            newFile +=  ss + "\n";
            let j = 1;
            while (lines.length > i + j) {
              if (lines[i + j]) {
                newFile += lines[i + j] + "\n";
              } else {
                break;
              }
              j++;
            }
            newFile += "\n"
          }
          break;
      }
    }
    return newFile;
  }
}

module.exports = SubtitleSlicer;