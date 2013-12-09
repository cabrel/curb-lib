'use strict';

var nodemailer = require('nodemailer');
var Qbacklog = require('./backlog');
var config = require('cabrel-config')();
var moment = require('moment');
var stockpile = require('cabrel-stockpile');
var promise = require('when');


function Email(smtpServer, smtpPort) {
  this.client = config.buildRedisClient();
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

  var deferred = promise.defer(),
      message;

  if (!payload) {
    return promise.reject(new Error('Message to queue was missing'));
  }

  if (!stockpile.checks.isObject(payload)) {
    return promise.reject(new Error('Expected message to be an object'));
  }

  message = JSON.stringify(payload);

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
};


/**
 * [ description]
 * @param  {[type]} obj [description].
 * @return {[type]}     [description].
 */
Email.prototype.send_ = function(message) {
  var self = this;
  var deferred = promise.defer();

  if (!message) {
    return promise.reject(new Error('Mail message was missing'));
  }

  if (stockpile.checks.isObject(message)) {
    return promise.reject(new Error('Expected message to be an object'));
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
};

Email.prototype.log = function(message) {
  var self = this,
      parsedMessage;

  if (!message) {
    throw new Error('Message missing');
  }

  if (!stockpile.checks.isObject(message)) {
    throw new Error('Expected message to be an object');
  }

  message.sentOn = moment().unix();

  if (stockpile.checks.isDefined(message.transport)) {
    delete message.transport;
  }

  parsedMessage = JSON.stringify(message);

  self.client.rpush(config.config.prefix + ':email:log', parsedMessage);
};

module.exports = Email;
