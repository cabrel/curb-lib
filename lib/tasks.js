'use strict';

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
  this.backlog.queue(this.queueName, JSON.stringify(item));

  if (!this.watching_) {
    this.process_();
  }
};

Tasks.prototype.process_ = function() {
  var self = this;

  this.id_ = setInterval(function() {
    self.watching_ = true;
    var localJob = null;

    self.backlog.pop(self.queueName).done(function(result) {
      localJob = result;

      if (localJob) {
        var task = JSON.parse(localJob);
        // sadly, because Hapi's server.inject method only returns a single value,
        // anything that expects normal nodejs callbacks will think of it as an error
        self.targetObject_[self.targetMethod_](task, self.complete_);
      }
    }, function(error) {
      console.log('Tasks process_ error', error);
      console.log('Error occurred processing the job ', localJob);
      clearInterval(self.id_);
    });
  }, 5000);
};

Tasks.prototype.complete_ = function(task) {
  console.log('Task complete ', task.result);
};

module.exports = Tasks;
