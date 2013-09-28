var nodemailer = require('nodemailer');
var NodeUtil = require('util');
var Q = require('q');
var Qbacklog = require('./backlog');
var loader = require('cabrel-config')();
var msgpack = require('msgpack');
var moment = require('moment');
var Uuid = require('node-uuid');


function Email(smtpServer, smtpPort) {
  this.client = null;
  this.backlog = new Qbacklog();

  this.SmtpServer = smtpServer;
  this.SmtpPort = smtpPort;

  this.Transport = nodemailer.createTransport('SMTP', {
    host: smtpServer,
    port: smtpPort
  });
}


/**
 * [ description]
 *
 * @param  {[type]} obj [description].
 *
 * @return {[type]}     [description].
 */
Email.prototype.queue = function(payload) {
  var self = this;

  return Q.fcall(function() {
    var deferred = Q.defer(),
        message;

    if (!payload) {
      throw new Error('Message to queue was missing');
    }

    if (typeof payload !== 'object') {
      throw new Error('Expected message to be an object');
    }

    message = JSON.stringify(payload);

    if (!self.client || !self.client.connected) {
      self.client = loader.buildRedisClient();
    }

    self.client.publish('email', message, function(err, recv) {
      if (err) {
        deferred.reject(err);
      } else {
        if (recv === 0) {
          self.backlog.queue('email', message);
          deferred.resolve(false);
        } else {
          deferred.resolve(true);
        }

      }
    });

    return deferred.promise;
  });
};


/**
 * [ description]
 * @param  {[type]} obj [description].
 * @return {[type]}     [description].
 */
Email.prototype.send_ = function(message) {
  var self = this;

  return Q.fcall(function() {
    var deferred = Q.defer();

    if (!message) {
      throw new Error('Mail message was missing');
    }

    if (typeof message !== 'object') {
      throw new Error('Expected message to be an object');
    }

    if (!self.Transport) {
      self.Transport = nodemailer.createTransport('SMTP', {
        host: self.SmtpServer,
        port: self.SmtpPort
      });
    }

    self.Transport.sendMail(message, function(err, response) {
      if (err) {
        self.backlog.queue('email', message); // place the message in queue if the send failed for any reason
        deferred.reject(err);
      } else {
        self.log(message);
        deferred.resolve(response.message);
      }
    });

    return deferred.promise;
  });
};

Email.prototype.log = function(message) {
  var self = this;

  if (!message) {
    throw new Error('Message missing');
  }

  if (typeof message !== 'object') {
    throw new Error('Expected message to be an object');
  }

  if (!self.client || !self.client.connected) {
    self.client = loader.buildRedisClient();
  }

  message.sentOn = moment().unix();

  self.client.rpush(loader.config.prefix + ':email:log', JSON.stringify(message));
};

module.exports = Email;
