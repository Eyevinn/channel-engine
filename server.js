const ChannelEngine = require('./index.js');
const AssetManager = require('./assetmanagers/default.js');

const ASSETMGR_URI = process.env.ASSETMGR_URI;
const ADCOPYMGR_URI = process.env.ADCOPYMGR_URI;
const ADXCHANGE_URI = process.env.ADXCHANGE_URI;

if (!ASSETMGR_URI) {
  console.error("An ASSETMGR_URI must be specified");
} else {
  const assetManager = new AssetManager(ASSETMGR_URI);
  const engine = new ChannelEngine(assetManager, { adCopyMgrUri: ADCOPYMGR_URI, adXchangeUri: ADXCHANGE_URI });
  engine.listen(process.env.PORT || 8000);
}