// Functions for adapting EventEmitters.

var EventEmitter = require('events').EventEmitter;

// Adapt an event emitter such that it translates between event names.
// For example, supplying `{'message': 'data'}` as the map would
// result in all `'message'` events being emitted as
// `'data'` events (with the same arguments).
function rename(events, map) {
  for (var event in map) {
    var t = map[event];
    events.on(event, function() {
      var ev = arguments.slice();
      ev.unshift(t);
      events.emit.apply(that, ev);
    });
  }
}
