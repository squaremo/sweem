var streams = require('./lib/streams');

// Make the onboard operator the top-level function, for convenience.
module.exports = streams.source;
module.exports.queue = streams.queue;
module.exports.join = streams.join;
module.exports.mux = streams.mux;
module.exports.demux = streams.demux;
// All others are left as methods of tuple stream only.
