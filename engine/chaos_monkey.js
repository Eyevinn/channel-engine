const debug = require('debug')('chaos-monkey');

const CHAOS_MONKEY_PROBABILITY = process.env.CHAOS_MONKEY_PROBABILITY || 0.5;
const doChaos = () => process.env.CHAOS_MONKEY === "true" && Math.random() < CHAOS_MONKEY_PROBABILITY;

const loadVod = async (p) => {
  if (doChaos()) {
    debug("Chaos Monkey is doing stuff");
    throw new Error("Chaos monkey prevented you from loading a VOD");
  } else {
    return await p;
  }
};

module.exports = {
  loadVod
};