var _ = require('lodash');
var NodeUtil = require('util');
var Q = require('q');
var loader = require('cabrel-config')();
var msgpack = require('msgpack');

function Backlog() {
  this.client = null;
  this.BACKLOG_SUFFIX = 'backlog';
}


/**
 * [description]
 *
 * @param  {[type]} rootQueue
 *
 * @return {[type]}
 */
Backlog.prototype.len = Q.fbind(function(rootQueue) {
  if (!rootQueue) {
    throw new Error('Root queue is missing');
  }

  if (!this.client || !this.client.connected) {
    this.client = loader.buildRedisClient();
  }

  return Q.npost(this.client, 'llen', [loader.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX]);
});


/**
 * [queue description]
 *
 * @param  {[type]} rootQueue
 * @param  {[type]} message
 *
 * @return {[type]}
 */
Backlog.prototype.queue = function(rootQueue, message) {
  var parsedMessage,
      self = this;

  if (!rootQueue) {
    throw new Error('Root queue is missing');
  }

  if (!message) {
    throw new Error('Message is missing');
  }

  if (!this.client || !this.client.connected) {
    this.client = loader.buildRedisClient();
  }

  // if the message isn't a string already, convert it
  if (typeof message === 'object' && !(message instanceof Buffer)) {
    if (typeof message.transport !== 'undefined') {
      delete message.transport;
    }

    parsedMessage = msgpack.pack(message);
  } else {
    parsedMessage = message;
  }

  this.client.rpush(loader.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, parsedMessage);
};


/**
 * [pop description]
 *
 * @param  {[type]} rootQueue
 *
 * @return {[type]}
 */
Backlog.prototype.pop = function(rootQueue) {
  var deferred = Q.defer(),
      self = this;

  if (!rootQueue) {
    deferred.reject('Root queue name is missing');
  } else {

    if (!this.client || !this.client.connected) {
      this.client = loader.buildRedisClient();
    }

    this.client.lpop(loader.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, function(err, value) {
      if (err) {
        deferred.reject(err);
      } else {
        deferred.resolve(value);
      }
    });
  }

  return deferred.promise;
};

module.exports = Backlog;
