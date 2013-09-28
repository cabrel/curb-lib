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
Backlog.prototype.len = function(rootQueue) {
  var self = this;

  return Q.fcall(function() {
    if (!rootQueue) {
      throw new Error('Root queue is missing');
    }

    if (!self.client || !self.client.connected) {
      self.client = loader.buildRedisClient();
    }

    return Q.npost(self.client, 'llen', [loader.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX]);
  });
};


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

  if (typeof message === 'object') {
    if (typeof message.transport !== 'undefined') {
      delete message.transport;
    }

    parsedMessage = JSON.stringify(message);
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
  var self = this;

  return Q.fcall(function() {
    var deferred = Q.defer();

    if (!rootQueue) {
      throw new Error('Root queue name is missing');
    }

    if (!self.client || !self.client.connected) {
      self.client = loader.buildRedisClient();
    }

    self.client.lpop(loader.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX, function(err, value) {
      if (err) {
        deferred.reject(err);
      } else {
        deferred.resolve(value);
      }
    });

    return deferred.promise;
  });
};

module.exports = Backlog;
