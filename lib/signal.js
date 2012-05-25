// Signals: these are just values. But you can pipe into them, and use
// `changes` or `sample` to get an event stream again. Some signals
// have other fun methods.

var streams = require('./streams');

function Signal(initial) {
  this._value = initial;
  var that = this;
  streams.Queue.call(function(data) {
    that.value(data);
  });
  this.readable = false;
}
Signal.prototype = new streams.Queue();

module.exports.signal = function(initial) {
  return new Signal(initial);
}

Signal.prototype.value = function() {
  if (arguments.length < 1) {
    return this._value;
  }
  else {
    var data = arguments[0];
    if (data != this._value) {
      var old = this._value;
      this._value = data;
      this.emit('change', data, old);
    }
  }
};
Signal.prototype.changes = function() {
  // FIXME can we propagate backpressure
  var out = new streams.Queue();
  this.on('change', function(data) {
    out.write(data);
  });
  return out;
};
// output the value every n milliseconds
Signal.prototype.sample = function(n) {
  var that = this;
  var q = new streams.Queue();
  var to = setTimeout(function fire() {
    q.write(that.value());
    to = setTimeout(fire, n);
  }, n);
  q.stop = function() { clearTimeout(to); };
  return q;
};
