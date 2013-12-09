'use strict';

var promise = require('when');
var config = require('cabrel-config')();
var stockpile = require('cabrel-stockpile');
var nodefn = require('when/node/function');

var Backlog = module.exports = function Backlog() {
  this.client = config.buildRedisClient();
  this.BACKLOG_SUFFIX = 'backlog';
  this._LOCALQUEUE = [];
  this._watcherId = null;
};


/**
 * [description]
 *
 * @param  {[type]} rootQueue
 *
 * @return {[type]}
 */
Backlog.prototype.len = function(rootQueue) {
  var self = this;

  var deferred = promise.defer();

  if (!rootQueue) {
    return promise.reject(new Error('Root queue is missing'));
  }

  self.client.llen(config.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX, function(err, result) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(result);
    }
  });

  return deferred.promise;
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

  if (stockpile.checks.isObject(message)) {
    if (stockpile.checks.isDefined(message.transport)) {
      delete message.transport;
    }

    parsedMessage = JSON.stringify(message);
  } else {
    parsedMessage = message;
  }

  this._LOCALQUEUE.push(['RPUSH', config.config.prefix + ':' + rootQueue + ':' + this.BACKLOG_SUFFIX, parsedMessage]);

  if (this._watcherId === null) {
    this._watcherId = setInterval(function() {
      self._storeQueue(function(err, result) {
        if (err) {
          console.log(err);
        } else {

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
  var deferred = promise.defer();

  if (this._LOCALQUEUE.length > 0) {
    var multi = this.client.multi(this._LOCALQUEUE);

    multi.exec(function(err, result) {
      if (err) {
        deferred.reject(err);
      } else {
        deferred.resolve(result);
      }

      self._LOCALQUEUE = [];
    });

    nodefn.bindCallback(deferred.promise, callback);
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
  var deferred = promise.defer();

  if (!rootQueue) {
    return promise.reject(new Error('Root queue name is missing'));
  }

  self.client.lpop(config.config.prefix + ':' + rootQueue + ':' + self.BACKLOG_SUFFIX, function(err, result) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(result);
    }
  });

  return deferred.promise;
};
