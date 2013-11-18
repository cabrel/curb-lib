var _ = require('lodash');
var NodeUtil = require('util');
var Q = require('q');
var loader = require('cabrel-config')();
var msgpack = require('msgpack');

function Backlog() {
  this.client = loader.buildRedisClient();
  this.BACKLOG_SUFFIX = 'backlog';
  this._LOCALQUEUE = [];
  this._watcherId = null;
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
    var deferred = Q.defer();

    if (!rootQueue) {
      throw new Error('Root queue is missing');
    }

    self.client.llen(loader.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX, function(err, result) {
      if (err) {
        deferred.reject(err);
      } else {
        deferred.resolve(result);
      }
    });

    return deferred.promise;
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

  if (typeof message === 'object') {
    if (typeof message.transport !== 'undefined') {
      delete message.transport;
    }

    parsedMessage = JSON.stringify(message);
  } else {
    parsedMessage = message;
  }

  //this.client.rpush(loader.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, parsedMessage);
  this._LOCALQUEUE.push(['RPUSH', loader.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, parsedMessage]);

  if (this._watcherId === null) {
    this._watcherId = setInterval(function() {
      console.log('Processing local queue');
      self._storeQueue(function(err, result) {
        if (err) {
          console.log(err);
        } else {
          console.log('local queue watcher result: ', result);

          if (result === false) {
            clearInterval(self._watcherId);
            self._watcherId = null;
          }
        }
      });
    }, 10000);
  }

};


/**
 *  [_storeQueue description]
 *
 *  @param     {Function}    callback
 *
 *  @return    {[type]}
 */
Backlog.prototype._storeQueue = function(callback) {
  var self = this;

  if (this._LOCALQUEUE.length > 0) {
    var multi = this.client.multi(this._LOCALQUEUE);
    Q.npost(multi, 'exec', []).done(function(result) {
      self._LOCALQUEUE = [];
      return callback(null, true);
    }, function(error) {
      return callback(error, null);
    });
  } else {
    return callback(null, false);
  }

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

    self.client.lpop(loader.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX, function(err, result) {
      if (err) {
        deferred.reject(err);
      } else {
        deferred.resolve(result);
      }
    });

    return deferred.promise;
  });
};

module.exports = Backlog;
