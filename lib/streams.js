// Functions for manipulating Streams.

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
// (which also follow the backpressure protocol).
function TupleStream() {
  Stream.call(this);
  this.writable = true;
  this.readable = true;
  this._running = true;
  this._buffer = [];
}
TupleStream.prototype = new Stream();
TupleStream.prototype.write = function() {
  debug(cons('write', arguments));
  if (this._running) {
    this.emit.apply(this, cons('data', arguments));
  }
  else {
    this._buffer.push(arguments);
  }
  return this._running;
}

TupleStream.prototype._flushBuffer = function() {
  var buf = this._buffer;
  var datum;
  while (this._running && (datum = buf.shift())) {
    this.emit.apply(this, cons('data', datum));
  }
  if (buf.length === 0) {
    this.emit('drain');
  }
}

TupleStream.prototype.pause = function() {
  this._running = false;
}

TupleStream.prototype.resume = function() {
  this._running = true;
  this._flushBuffer();
}

// Largely replicated from Stream#pipe, except for the ondata helper
// which must write tuples rather than a single datum.
TupleStream.prototype.pipe = function(dest, options) {
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

// Create an enhanced stream from a regular stream; this is also made the
// top-level export for convenience.
module.exports = function(stream) {
  return stream.pipe(new TupleStream());
}
// For those that like it explicit.
module.exports.source = module.exports;

// For subclassings
module.exports.TupleStream = TupleStream;

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
function Join() {
  var streams = [].slice.call(arguments);
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
          if (false === that.write.apply(that, args)) {
            streams.forEach(function(s) { s.pause && s.pause(); });
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
    streams.forEach(function(s) { s.resume && s.resume(); });
  });

  // TODO cleanup and on('error', ...)

  return this;
}
Join.prototype = new TupleStream();

module.exports.join = function() {
  var j = new TupleStream();
  Join.apply(j, arguments);
  return j;
}

// === Stream operators

// FIXME the way I've done these, backpressure won't propagate. I need
// to either use an abstract method, or provide an `upstream` helper
// in TupleStream.

// Multiplex a set of data streams into one, in essence labelling each
// data event with the label given to the stream. Returns a MultiStream
// carrying all the events.
function mux(map) {
  var proxy = new TupleStream();
  function handler(label) {
    return function(data) {
      proxy.emit('data', {label: data});
    }
  }
  for (var label in map) {
    map[label].on('data', handler(label));
  }
  return proxy;
}

module.exports.mux = mux;

// Take a muxed stream and return a tuple stream that emits events
// corresponding to the labels given to the original streams. For
// example,
//
//    var m = mux({'notify': stream1, 'alert': stream2});
//    var dm = demux(m);
//    dm.on('notify', function(data) {...});
//    dm.on('alert', function(data) {...});
//
function demux(muxed) {
  var demuxed = new ProxyStream();
  muxed.on('data', function(event) {
    for (var label in event) {
      return demuxed.emit(label, event[label]);
    }
  });
}

module.exports.demux = demux;

// Lift a stream supplying n-elements to a stream supplying n-element
// maps.
function lift(stream /* , keys */) {
  var keys = [].slice.call(arguments, 1);
  var proxy = new TupleStream();
  stream.on('data', function() {
    var out = {};
    for (var i in keys) {
      out[keys[i]] = arguments[i];
    }
    return proxy.write(out);
  });
  return proxy;
}

module.exports.lift = lift;
TupleStream.prototype.lift = function() {
  return lift.apply(null, cons(this, arguments));
}

// Drop a stream supplying n-element maps into a stream supplying
// n-elements.
function drop(stream /*, keys */) {
  var keys = [].slice.call(arguments, 1);
  var proxy = new TupleStream();
  stream.on('data', function(data) {
    var args = ['data'];
    for (var k in keys) {
      args.push(data[k]);
    }
    proxy.write.apply(proxy, args);
  });
}

module.exports.drop = drop;
TupleStream.prototype.drop = function() {
  return drop.apply(null, cons(this, arguments));
}

// Partition a stream into data for which the predicate returns true
// and data for which it doesn't. The latter is emitted using the
// event 'notdata', which may be ignored in order to filter the
// stream.
function partition(stream, predicate) {
  var proxy = new TupleStream();
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

  stream.on('data', function(data) {
    if (pred(data)) {
      proxy.write(data);
    }
    else {
      // or another stream?
      proxy.emit('notdata', data);
    }
  });
  return proxy;
}

module.exports.partition = module.exports.filter = partition;
TupleStream.prototype.filter = TupleStream.prototype.partition =
  function(predicate) {
    return partition.call(null, this, predicate);
  };

function map(stream, transform) {
  var proxy = new TupleStream();
  stream.on('data', function() {
    proxy.write(transform.call(transform, arguments));
  });
  return proxy;
}

module.exports.map = map;
TupleStream.prototype.map = function(transform) {
  return map(this, transform);
};

// The arguments of fold and its func argument are in an unusual
// order, to be consistent with other operators in this module, and to
// allow multiple data arguments.
// func :: accumulator x data_0 ... x data_n -> accumulator
function fold(stream, func, init) {
  var s = new TupleStream();
  var val = init;
  stream.on('data', function() {
    val = func.apply(func, cons(val, arguments));
    s.write(val);
  });
  return s;
}

module.exports.fold = fold;
TupleStream.prototype.fold = function(func, init) {
  return fold(this, func, init);
};
