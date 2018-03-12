const ChannelEngine = require('./index.js');

const ASSETMGR_URI = process.env.ASSETMGR_URI;
const ADCOPYMGR_URI = process.env.ADCOPYMGR_URI;

if (!ASSETMGR_URI) {
  console.error("An ASSETMGR_URI must be specified");
} else {
  const engine = new ChannelEngine(ASSETMGR_URI, ADCOPYMGR_URI);
  engine.listen(process.env.PORT || 8000);
}