// Polyadic Streams and operators therein.

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

// A base for objects that will accept and emit tuples. Back-pressure
// is propagated with the same protocol to streams; Streams themselves
// may be "onboarded" by using Source, and decanted by using Sink
// (which also follow the backpressure protocol). NB although this
// implements Stream (a possibly duplex channel), it's really a queue
// (what you write is readable at the other end). Stream is
// implemented so that it can participate in Stream's pipe protocol.
function TupleQueue(handleIncoming) {
  Stream.call(this);
  // If not given a handler, behave like a queue, passing data on via
  // emitData. Any handler supplied ought to use emitData also.
  this._handle = handleIncoming || function() {
    return this.emitData.apply(this, arguments);
  };
  this.writable = true;
  this.readable = true;
  this._running = true;
  this._buffer = [];
}
TupleQueue.prototype = new Stream();

TupleQueue.prototype.emitData = function() {
  if (this._running) {
    this.emit.apply(this, cons('data', arguments));
  }
  else {
    this._buffer.push(arguments);
  }
  return this._running;
}
TupleQueue.prototype.write = function() {
  return this._handle.apply(this, arguments);
}

TupleQueue.prototype._flushBuffer = function() {
  var buf = this._buffer;
  var datum;
  while (this._running && (datum = buf.shift())) {
    this.emit.apply(this, cons('data', datum));
  }
  if (buf.length === 0) {
    this.emit('drain');
  }
}

TupleQueue.prototype.pause = function() {
  this._running = false;
}

TupleQueue.prototype.resume = function() {
  this._running = true;
  this._flushBuffer();
}

// Largely replicated from Stream#pipe, except for the ondata helper
// which must write tuples rather than a single datum.
TupleQueue.prototype.pipe = function(dest, options) {
  var source = this;

  function ondata() {
    if (dest.writable) {
      if (false === dest.write.apply(dest, arguments) &&
          source.pause) {
        source.pause();
      }
    }
  }

  source.on('data', ondata);

  function ondrain() {
    if (source.readable && source.resume) {
      source.resume();
    }
  }

  dest.on('drain', ondrain);

  // If the 'end' option is not supplied, dest.end() will be called when
  // source gets the 'end' or 'close' events.  Only dest.end() once.
  if (!dest._isStdio && (!options || options.end !== false)) {
    source.on('end', onend);
    source.on('close', onclose);
  }

  var didOnEnd = false;
  function onend() {
    if (didOnEnd) return;
    didOnEnd = true;
    // remove the listeners
    cleanup();
    dest.end();
  }

  function onclose() {
    if (didOnEnd) return;
    didOnEnd = true;
    // remove the listeners
    cleanup();
    dest.destroy();
  }

  // don't leave dangling pipes when there are errors.
  function onerror(er) {
    cleanup();
    if (this.listeners('error').length === 0) {
      throw er; // Unhandled stream error in pipe.
    }
  }

  source.on('error', onerror);
  dest.on('error', onerror);

  // remove all the event listeners that were added.
  function cleanup() {
    source.removeListener('data', ondata);
    dest.removeListener('drain', ondrain);

    source.removeListener('end', onend);
    source.removeListener('close', onclose);

    source.removeListener('error', onerror);
    dest.removeListener('error', onerror);

    source.removeListener('end', cleanup);
    source.removeListener('close', cleanup);

    dest.removeListener('end', cleanup);
    dest.removeListener('close', cleanup);
  }

  source.on('end', cleanup);
  source.on('close', cleanup);

  dest.on('end', cleanup);
  dest.on('close', cleanup);

  dest.emit('pipe', source);

  // Allow for unix-like usage: A.pipe(B).pipe(C)
  return dest;
};

// TODO end, close, destroy, destroySoon .. possibly others

// Create a tuple stream from a regular stream.
module.exports.source = function(stream) {
  return stream.pipe(new TupleQueue());
};
// Construct a queue (i.e., a bare TupleQueue)
module.exports.queue = function() {
  return new TupleQueue();
};

// For subclassings
module.exports.TupleQueue = TupleQueue;

// Stream operators
// ================

// These functions combine streams in some way, rather than
// transforming a single stream. For this reason they stand apart from
// the TupleQueue methods.


// Base for things that are writable themselves, but accept data from
// multiple streams. Since we won't be piped into, but rather will
// subscribe to data, we need to implement the backpressure trigger
// ourselves rather than leaving it to the upstream er, stream.
function Combine(streams) {
  TupleQueue.call(this);
  this._streams = streams;
  this.writable = false;
}
Combine.prototype = new TupleQueue();
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
//     join(stream1, stream2).on('data', function(data1, data2) {...});
//
// the callback will get called when data has arrived at both of
// stream1 and stream2.
function Join(maybeArray) {
  // So we can call the constructor with an arbitrary list of arguments,
  // allow it to be an array.
  // Except we're not writable, since not really a queue.
  var streams = (Array.isArray(maybeArray)) ?
    maybeArray : [].slice.call(arguments);
  Combine.call(this, streams);

  debug("streams: " + streams.length);
  var queues = new Array(streams.length);
  var bits = 0;
  var full = (1 << (streams.length)) - 1;
  var that = this;

  // We are effectively piping from each upstream to this; we have to
  // implement the same bits and pieces as pipe does, except we are
  // dest rather than source. (Largely TODO)

  function handler(index) {
    var bit = 1 << index;
    return function(data) {
      debug("data arrived at " + index);
      if (bits & bit) {
        debug("already had one");
        // already a queued value; just queue another
        queues[index].push(data);
      }
      else {
        var newbits = bits | bit;
        if (newbits == full)  {
          debug("full!");
          var args = new Array(queues.length);
          for (var j in queues) {
            if (j == index) {
              args[j] = data;
            }
            else {
              args[j] = queues[j].shift();
              if (queues[j].length == 0) {
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

  for (var index in streams) {
    queues[index] = [];
    var stream = streams[index];
    stream.on('data', handler(index));
  }

  this.on('drain', function() {
    that.resumeUpstreams();
  });

  // TODO cleanup and on('error', ...)
}
Join.prototype = new Combine();

module.exports.join = function() {
  return new Join([].slice.call(arguments));
}


// Multiplex a set of data streams into one, labelling each data event
// with the label given to the stream from whence it came. Returns a
// tuple stream carrying all the events.
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
    streams.push(map[label]);
    map[label].on('data', handler(label));
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
  var out = new TupleQueue(function(event) {
    // Assume there's only one, but no harm if not.
    for (var label in event) {
      out.emit(label, event[label]);
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
// procedures and as chainable methods of TupleQueue.

// Lift a stream supplying n-elements to a stream supplying n-element
// maps.
function lift(stream /* , keys */) {
  var keys = [].slice.call(arguments, 1);
  var q = new TupleQueue(function() {
    var out = {};
    for (var i in keys) {
      out[keys[i]] = arguments[i];
    }
    return q.emitData(out);
  });
  stream.pipe(q);
  return q;
}

module.exports.lift = lift;
TupleQueue.prototype.lift = function() {
  return lift.apply(null, cons(this, arguments));
}

// Drop a stream supplying n-element maps into a stream supplying
// n-elements.
function drop(stream /*, keys */) {
  var keys = [].slice.call(arguments, 1);
  var q = new TupleQueue(function(data) {
    console.log({'data': data, 'keys': keys});
    var args = [];
    for (var i in keys) {
      args.push(data[keys[i]]);
    }
    return q.emitData.apply(q, args);
  });
  stream.pipe(q);
  return q;
}

module.exports.drop = drop;
TupleQueue.prototype.drop = function() {
  return drop.apply(null, cons(this, arguments));
}

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

  var q = new TupleQueue(function(data) {
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
TupleQueue.prototype.filter = TupleQueue.prototype.partition =
  function(predicate) {
    return partition.call(null, this, predicate);
  };

// Apply a function to all data coming in. The return value is emitted
// downstream, even if it's null or undefined.
function map(stream, transform) {
  var q = new TupleQueue(function() {
    return q.emitData(transform.apply(null, arguments));
  });
  stream.pipe(q);
  return q;
}

module.exports.map = map;
TupleQueue.prototype.map = function(transform) {
  return map(this, transform);
};

// The arguments of fold and its func argument are in an unusual
// order, to be consistent with other operators in this module, and to
// allow multiple data arguments. The accumulator is emitted.
//  func :: accumulator x data_0 ... x data_n -> accumulator
function fold(stream, func, init) {
  var val = init;
  var q = new TupleQueue(function() {
    val = func.apply(func, cons(val, arguments));
    return q.emitData(val);
  });
  stream.pipe(q);
  return q;
}

module.exports.fold = fold;
TupleQueue.prototype.fold = function(func, init) {
  return fold(this, func, init);
};

// This is a bit specialised. The idea is to have a small protocol for
// parsing streams of bytes -- e.g., what you get from a socket --
// into frames, of variable size, which are not aligned with the
// incoming buffers.  This is a bit like fold, except the procedure
// indicates whether it wants to emit a value or not in the return
// value:
// func :: accumulator x data_0 ... x data_n ->
//            {value: val, rest: accumulator}
//          | {rest: accumulator}

function frames(stream, func, zero) {
  var accum = zero;
  function parse() {
    var result = func.apply(func, cons(accum, arguments));
    accum = result.rest;
    if ('value' in result) {
      q.emitData(result.value);
    }
  }
  var q = new TupleQueue(parse);
  stream.pipe(q);
  return q;
}
TupleQueue.prototype.frames = function(func, init) {
  return frames(this, func, init);
}

// shortcuts for JSON-codec
TupleQueue.prototype.tojson = function() {
  return this.map(JSON.stringify);
};
TupleQueue.prototype.fromjson = function() {
  return this.map(JSON.parse);
};
