var _ = require('lodash');
var NodeUtil = require('util');
var Q = require('q');
var QBacklog = require('./backlog');

function Tasks(target, method, queueName) {
  this.targetObject_ = target;
  this.targetMethod_ = method;
  this.watching_ = false;
  this.id_ = null;
  this.queueName = queueName;

  this.backlog = new QBacklog();
}

Tasks.prototype.push = function(item) {
  var self = this;

  this.backlog.queue(self.queueName, JSON.stringify(item));

  if (!this.watching_) {
    this.process_();
  }
};

Tasks.prototype.process_ = function() {
  var self = this;


  this.id_ = setInterval(function() {
    self.watching_ = true;

    self.backlog.pop(self.queueName).done(function(result) {
      if (result) {
        var task = JSON.parse(result);
        // sadly, because Hapi's server.inject method only returns a single value,
        // anything that expects normal nodejs callbacks will think of it as an error
        self.targetObject_[self.targetMethod_](task, self.complete_);
      }
    }, function(error) {
      console.log(error);
      clearInterval(self.id_);
    });

    // }
  }, 5000);
};

Tasks.prototype.complete_ = function(task) {
  console.log(task.result);
};

module.exports = Tasks;
