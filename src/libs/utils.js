var Person = require('../models/person');
var Q = require('q');

module.exports = {
  getTypearea: function (hospcode, pid) {
    var q = Q.defer();

    Person.findOne({HOSPCODE: hospcode, PID: pid}, 'TYPEAREA', function (err, doc) {
      if (err) {
        q.reject(err);
      } else {
        if (doc) {
          q.resolve(doc.TYPEAREA);
        } else {
          q.resolve(null);
        }

      }
    });

    return q.promise;
  }
};