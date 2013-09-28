var nodemailer = require('nodemailer');
var NodeUtil = require('util');
var Q = require('q');
var Qbacklog = require('./backlog');
var loader = require('cabrel-config')();
var msgpack = require('msgpack');


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
  var deferred = Q.defer(),
      message,
      self = this;

  if (!payload) {
    deferred.reject('Message to queue was missing');
  } else {

    if (typeof payload === 'object') {
      message = msgpack.pack(payload);
    } else {
      message = payload;
    }

    if (!this.client || !this.client.connected) {
      this.client = loader.buildRedisClient();
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
  }

  return deferred.promise;
};


/**
 * [ description]
 * @param  {[type]} obj [description].
 * @return {[type]}     [description].
 */
Email.prototype.send_ = function(obj) {
  var self = this;
  var deferred = Q.defer();

  if (!obj) {
    deferred.reject('Mail message was missing');
  } else {
    if (!this.Transport) {
      this.Transport = nodemailer.createTransport('SMTP', {
        host: self.SmtpServer,
        port: self.SmtpPort
      });
    }

    this.Transport.sendMail(obj, function(err, response) {
      if (err) {
        self.backlog.queue('email', obj); // place the message in queue if the send failed for any reason
        deferred.reject(err);
      } else {
        deferred.resolve(response.message);
      }
    });
  }

  return deferred.promise;
};

module.exports = Email;
