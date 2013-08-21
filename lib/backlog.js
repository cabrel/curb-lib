var _ = require('lodash'),
    NodeUtil = require('util'),
    Q = require('q'),
    loader = require('cfts-config').loader;

function Backlog() {
  this.client = null;
  this.KEY_PREFIX = 'CURB';
  this.BACKLOG_SUFFIX = 'BACKLOG';
}


Backlog.prototype.len = Q.fbind(function(rootQueue) {
  if (!rootQueue) {
    throw new Error('Root queue is missing');
  }

  if (!this.client || !this.client.connected) {
    this.client = loader.buildRedisClient();
  }

  return Q.npost(this.client, 'llen', [this.KEY_PREFIX + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX]);
});

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
  if (typeof message !== 'string') {
    if ('transport' in message) {
      delete message.transport;
    }

    console.log(NodeUtil.inspect(message, true, null));
    parsedMessage = JSON.stringify(message);
  } else {
    parsedMessage = message;
  }

  this.client.rpush(this.KEY_PREFIX + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, parsedMessage);
};


Backlog.prototype.pop = function(rootQueue) {
  var deferred = Q.defer(),
      self = this;

  if (!rootQueue) {
    deferred.reject('Root queue name is missing');
  } else {

    if (!this.client || !this.client.connected) {
      this.client = loader.buildRedisClient();
    }

    this.client.lpop(this.KEY_PREFIX + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, function(err, value) {
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
