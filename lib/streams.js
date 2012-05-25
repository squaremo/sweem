// Tools for streams.

// We are principally interested in 'data' events; however we aim to
// do the right thing for the other Stream interface events and
// methods.

var Stream = require('stream').Stream;

// === Halpers

var debug = (process.env['DEBUG']) ?
  function (m) { console.log(m); } : function() {};

function cons(head, tail) {
  debug("[" + head + "| " + tail + ']');
  var list = Array.prototype.slice.call(tail);
  list.unshift(head);
  return list;
}

// === Stream constructors

// A base for objects that will both accept and emit values;
// essentially, the converse to a pipe (instead of writing when the
// source emits, it emits when you write).
//
// Back-pressure is propagated with the same protocol to streams;
// Streams themselves may be "onboarded" by using Source, and decanted
// by using Sink (which also follow the backpressure protocol).
// Although this implements Stream (a possibly duplex channel), it's
// really a queue (what you write is readable at the other
// end). Stream is implemented so that it can participate in Stream's
// pipe protocol.
function Queue(handleIncoming) {
  Stream.call(this);
  // If not given a handler, behave like a queue, passing data on via
  // emitData. Any handler supplied ought to use emitData also.
  this._handle = handleIncoming || function(data) {
    return this.emitData(data);
  };
  this.writable = true;
  this.readable = true;
  this._running = true;
  this._buffer = [];
}
Queue.prototype = new Stream();

Queue.prototype.emitData = function(data) {
  if (this._running) {
    this.emit('data', data);
  }
  else {
    this._buffer.push(data);
  }
  return this._running;
}
Queue.prototype.write = function(data) {
  return this._handle(data);
}

Queue.prototype._flushBuffer = function() {
  var buf = this._buffer;
  var datum;
  while (this._running && (datum = buf.shift())) {
    this.emit('data', datum);
  }
  if (buf.length === 0) {
    this.emit('drain');
  }
}

Queue.prototype.pause = function() {
  this._running = false;
}

Queue.prototype.resume = function() {
  this._running = true;
  this._flushBuffer();
}

// TODO end, close, destroy, destroySoon .. possibly others

// Entry point: give a stream and get a queue, with more goodies.
module.exports.source = function(stream) {
  return stream.pipe(new Queue());
};
// Construct a 'bare' queue for some reason
module.exports.queue = function() {
  return new Queue();
};

// For subclassings
module.exports.Queue = Queue;

// Stream operators
// ================

// These functions combine streams in some way, rather than
// transforming a single stream. For this reason they stand apart from
// the Queue methods.

// Base for things that are writable themselves, but accept data from
// multiple streams. Since we won't be piped into, but rather will
// subscribe to data, we need to implement the backpressure trigger
// ourselves rather than leaving it to the upstream er, stream.
function Combine(streams) {
  Queue.call(this);
  this._streams = streams;
  this.writable = false;
}
Combine.prototype = new Queue();
Combine.pauseUpstreams = function() {
  this._streams.forEach(function(s) { s.pause && s.pause(); });
};
Combine.resumeUpstreams = function() {
  this._streams.forEach(function(s) { s.resume && s.resume(); });
};

// Join a number of streams, such that the resulting tuple stream
// emits a data event with an argument from each stream when there is
// data available from every stream.
//
// E.g., in
//
//     join({one: stream1, two: stream2}).on('data',
//       function(dict) { /* something something dict.one, dict.two... */});
//
// i.e., the callback will get called when data has arrived at both of
// stream1 and stream2.

function Join(dict) {
  debug("streams: " + streams.length);
  var keys = [], streams = [];
  for (k in dict) {
    if (dict.hasOwnProperty(k)){
      keys.push(k);
      streams.push(dict[k]);
    }
  }
  Combine.call(this, streams);

  var count = keys.length;
  var queues = new Array(count);
  var bits = 0;
  var full = (1 << count) - 1;
  var that = this;

  // We are effectively piping from each upstream to this; we have to
  // implement the same bits and pieces as pipe does, except we are
  // dest rather than source. (Largely TODO)

  function handler(name, index) {
    // This bitset thing is pretty complicated, maybe I don't need it.
    var bit = 1 << index;
    return function(data) {
      debug("data arrived at " + name);
      if (bits & bit) {
        debug("already had one");
        // already a queued value; just queue another
        queues[index].push(data);
      }
      else {
        var newbits = bits | bit;
        if (newbits === full)  {
          debug("full!");
          var args = {};
          for (var j = 0; j < count; j++) {
            if (j === index) {
              args[name] = data;
            }
            else {
              args[keys[j]] = queues[j].shift();
              if (queues[j].length === 0) {
                bits -= (1 << j);
              }
            }
          }
          debug('args ' + args);
          // NB not just any old falsey value
          if (false === that.emitData.apply(that, args)) {
            that.pauseUpstreams();
          }
        }
        else {
          debug("not full: " + newbits);
          bits = newbits;
          queues[index].push(data);
        }
      }
    };
  }

  for (var index = 0; index < count; index++) {
    queues[index] = [];
    var stream = streams[index];
    stream.on('data', handler(keys[index], index));
  }

  this.on('drain', function() {
    that.resumeUpstreams();
  });

  // TODO cleanup and on('error', ...)
}
Join.prototype = new Combine();

module.exports.join = function(dict) {
  return new Join(dict);
}


// Multiplex a set of data streams into one, labelling each data event
// with the name given to its stream. Returns a tuple stream carrying
// all the events.
function Mux(map) {
  var that = this;
  var streams = [];
  function handler(label) {
    return function(data) {
      if (false === this.emitData({label: data})) {
        that.pauseUpstreams();
      }
    }
  }

  for (var label in map) {
    if (map.hasOwnProperty(label)) {
      streams.push(map[label]);
      map[label].on('data', handler(label));
    }
  }

  Combine.call(this, streams);
  this.on('drain', function() {
    that.resumeUpstreams();
  });
}
Mux.prototype = new Combine();

module.exports.mux = function(map) {
  return new Mux(map);
}


// Take a muxed stream and return a tuple stream that emits events
// corresponding to the labels given to the original streams. For
// example,
//
//    var m = mux({'notify': stream1, 'alert': stream2});
//    var dm = demux(m);
//    dm.on('notify', function(data) {...});
//    dm.on('alert', function(data) {...});
//
// NB this ends a pipeline, since it will in general no longer be
// emitting data events.
function demux(muxed) {
  var out = new Queue(function(event) {
    // Assume there's only one, but no harm if not.
    for (var label in event) {
      if (event.hasOwnProperty(label)) {
        out.emit(label, event[label]);
      }
    }
  });
  out.readable = false;
  return out;
}
module.exports.demux = demux;

// Stream transformers
// ===================
//
// These transform a stream of some product type into some other
// product type. In general they are supplied both as stand-alone
// procedures and as chainable methods of Queue.

// Partition a stream into data for which the predicate returns true
// and data for which it doesn't. The latter is emitted using the
// event 'notdata', which may be ignored in order to filter the
// stream.
function partition(stream, predicate) {
  var pred = predicate;
  switch (typeof predicate) {
  case 'string':
    // treat a string as a prefix
    var len = predicate.length;
    pred = function(data) {
      return data.substring(0, len) == predicate;
    };
    break;
  case 'function':
    break;
  case 'object':
    // Regular expressions and things like regular expressions
    if (predicate.test) {
      pred = function(data) {
        return predicate.test(data);
      };
      break; // break to right scope?
    }
  default:
    throw "I don't know how to use a predicate that's a " + (typeof predicate);
  }

  var q = new Queue(function(data) {
    if (pred(data)) {
      return q.emitData(data);
    }
    else {
      // or another stream?
      q.emit('notdata', data);
      return true;
    }
  });
  stream.pipe(q);
  return q;
}

module.exports.partition = module.exports.filter = partition;
Queue.prototype.filter = Queue.prototype.partition =
  function(predicate) {
    return partition.call(null, this, predicate);
  };

// Apply a function to all data coming in. The return value is emitted
// downstream, even if it's null or undefined.
function map(stream, transform) {
  var q = new Queue(function(data) {
    return q.emitData(transform(data));
  });
  stream.pipe(q);
  return q;
}

module.exports.map = map;
Queue.prototype.map = function(transform) {
  return map(this, transform);
};

//  func :: accumulator x data -> accumulator
function fold(stream, func, init) {
  var val = init;
  var q = new Queue(function(data) {
    val = func(val, data);
    return q.emitData(val);
  });
  stream.pipe(q);
  return q;
}

module.exports.fold = fold;
Queue.prototype.fold = function(func, init) {
  return fold(this, func, init);
};

// This is a bit specialised. The idea is to have a small protocol for
// parsing streams of bytes -- e.g., what you get from a socket --
// into frames, of variable size, which are not aligned with the
// incoming buffers.  This is a bit like fold, except the procedure
// indicates whether it wants to emit a value or not in the return
// value:
// parse :: data ->
//            {value: val, rest: accumulator}
//          | {rest: accumulator}
// combine :: accumulator x data -> data

function frames(stream, parse, combine, zero) {
  var accum = zero;
  function handle(newdata) {
    var data = combine(accum, newdata);
    var result = parse(data);
    while ('value' in result) {
      q.emitData(result.value);
      result = parse(result.rest);
    }
    accum = result.rest;
  }
  var q = new Queue(handle);
  stream.pipe(q);
  return q;
}


module.exports.frames = frames;
Queue.prototype.frames = function(parse, combine, zero) {
  return frames(this, parse, combine, zero);
}

// shortcuts for JSON-codec
Queue.prototype.tojson = function() {
  return this.map(JSON.stringify);
};
Queue.prototype.fromjson = function() {
  return this.map(JSON.parse);
};
