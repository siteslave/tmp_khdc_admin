var Q = require('q');

module.exports = {

  savePersonPyramid: function (knex, data) {
    var q = Q.defer();

    knex('s_person_pyramid')
      .delete()
      .then(function () {
        knex('s_person_pyramid')
          .insert(data)
          .then(function () {
            q.resolve();
          }, function (err) {
            q.reject(err);
          });
      }, function (err) {
        q.reject(err);
      });

    return q.promise;
  },
  savePersonSummary: function (knex, data) {
    var q = Q.defer();

    knex('s_person')
      .delete()
      .then(function () {
        knex('s_person')
          .insert(data)
          .then(function () {
            q.resolve();
          }, function (err) {
            q.reject(err);
          });
      }, function (err) {
        q.reject(err);
      });

    return q.promise;
  },
  saveDiagOpdSummary: function (knex, data) {
    var q = Q.defer();

    knex('s_diag_opd')
      .delete()
      .then(function () {
        knex('s_diag_opd')
          .insert(data)
          .then(function () {
            q.resolve();
          }, function (err) {
            q.reject(err);
          });
      }, function (err) {
        q.reject(err);
      });

    return q.promise;
  },
  saveLaborTransformPerson: function (knex, data) {
    var q = Q.defer();

    knex('t_person_labor')
      .delete()
      .then(function () {
        knex('t_person_labor')
          .insert(data)
          .then(function () {
            q.resolve();
          }, function (err) {
            q.reject(err);
          });
      }, function (err) {
        q.reject(err);
      });

    return q.promise;
  },
  saveServiceSummary: function (knex, data) {
    var q = Q.defer();

    knex('s_service')
      .delete()
      .then(function () {
        knex('s_service')
          .insert(data)
          .then(function () {
            q.resolve();
          }, function (err) {
            q.reject(err);
          });
      }, function (err) {
        q.reject(err);
      });

    return q.promise;
  }
};