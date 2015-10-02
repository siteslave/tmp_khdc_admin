var Q = require('q');
var _ = require('lodash');
var moment = require('moment');

module.exports = {
  personPyramid: function (db) {

    var q = Q.defer();
    var col = db.collection('person');

    // Create index
    col.createIndex({NATION: 1});
    col.createIndex({TYPEAREA: 1});

    col.aggregate([
      {$match: {NATION: "099", TYPEAREA: {$in: ["1", "3"]}}},
      {
        $group: {
          _id: {
            HOSPCODE: "$HOSPCODE",
            AGE_YEAR: {$subtract: [{$year: new Date()}, {$year: "$BIRTH"}]},
            SEX: "$SEX"
          },
          "TOTAL": {"$sum": 1}
        }
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      }
      else {
        //console.log(docs);
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var obj = {};
            obj.HOSPCODE = v._id.HOSPCODE;
            obj.AGE_YEAR = v._id.AGE_YEAR;
            obj.SEX = v._id.SEX;
            obj.TOTAL = v.TOTAL;
            obj.UPDATED_AT = moment().format('YYYY-MM-DD HH:mm:ss');

            data.push(obj);
          });
        }

        q.resolve(data);
      }
    });

    return q.promise;

  },

  personSummary: function (db) {

    var q = Q.defer();
    var col = db.collection('person');
    var date = new Date();
    // Create index
    col.createIndex({TYPEAREA: 1});

    col.aggregate([
      {
        $match: {"TYPEAREA": {$in: ["1", "3"]}}
      },
      {
        $group: {
          _id: {
            "HOSPCODE": "$HOSPCODE",
            "TYPEAREA": "$TYPEAREA",
            "SEX": "$SEX",
            "AGE_YEAR": {$subtract: [{$year: date}, {$year: "$BIRTH"}]}
          },
          "TOTAL": {"$sum": 1}
        }
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      }
      else {
        //console.log(docs);
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var obj = {};
            obj.HOSPCODE = v._id.HOSPCODE;
            obj.TYPEAREA = v._id.TYPEAREA;
            obj.AGE_YEAR = v._id.AGE_YEAR;
            obj.SEX = v._id.SEX;
            obj.TOTAL = v.TOTAL;
            obj.UPDATED_AT = moment().format('YYYY-MM-DD HH:mm:ss');

            data.push(obj);
          });
        }

        q.resolve(data);
      }
    });

    return q.promise;

  },

  serviceSummary: function (db) {

    var q = Q.defer();
    var col = db.collection('service');

    // Create index
    col.createIndex({DATE_SERV: 1});

    col.aggregate([
      //{
      //  $match: { DATE_SERV: { $gte: ISODate("2014-10-01"), $lte: ISODate("2015-09-30") }}
      //},
      {
        $group: {
          _id: {
            HOSPCODE: "$HOSPCODE",
            MONTH: {$month: "$DATE_SERV"},
            YEAR: {$year: "$DATE_SERV"},
            INSTYPE: "$INSTYPE"
          },
          "TOTAL": {"$sum": 1}
        }
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      } else {
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var obj = {};
            obj.HOSPCODE = v._id.HOSPCODE;
            obj.S_MONTH = v._id.MONTH;
            obj.S_YEAR = v._id.YEAR;
            obj.INSTYPE = v._id.INSTYPE;
            obj.TOTAL = v.TOTAL;
            obj.UPDATED_AT = moment().format('YYYY-MM-DD HH:mm:ss');

            data.push(obj);
          });
        }

        q.resolve(data);

      }

    });

    return q.promise;

  },

  diagOpdSummary: function (db) {

    var q = Q.defer();
    var col = db.collection('diagnosis_opd');

    col.aggregate([
      {
        $group: {
          _id: {
            HOSPCODE: "$HOSPCODE",
            MONTH: {$month: "$DATE_SERV"},
            YEAR: {$year: "$DATE_SERV"},
            DIAGTYPE: "$DIAGTYPE",
            DIAGCODE: "$DIAGCODE"
          },
          "TOTAL": {"$sum": 1}
        }
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      } else {
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var obj = {};
            obj.HOSPCODE = v._id.HOSPCODE;
            obj.S_MONTH = v._id.MONTH;
            obj.S_YEAR = v._id.YEAR;
            obj.DIAGCODE = v._id.DIAGCODE;
            obj.DIAGTYPE = v._id.DIAGTYPE;
            obj.TOTAL = v.TOTAL;
            obj.UPDATED_AT = moment().format('YYYY-MM-DD HH:mm:ss');

            data.push(obj);
          });
        }

        q.resolve(data);

      }

    });

    return q.promise;

  },

  laborTransformPerson: function (db, pids) {

    var q = Q.defer();
    var col = db.collection('person');

    col.aggregate([
      {
        $project: {
          HOSPCODE: "$HOSPCODE",
          PID: "$PID",
          NPID: {$concat: ["$HOSPCODE", "$PID"]},
          PTNAME: {$concat: ["$NAME", " ", "$LNAME"]},
          BIRTH: "$BIRTH",
          TYPEAREA: "$TYPEAREA"
        }
      },
      {
        $match: {NPID: {$in: pids}}
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      } else {
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var obj = {};
            obj.HOSPCODE = v.HOSPCODE;
            obj.PID = v.PID;
            obj.NPID = v.NPID;
            obj.PTNAME = v.PTNAME;
            obj.BIRTH = moment(v.BIRTH).format('YYYY-MM-DD');
            obj.TYPEAREA = v.TYPEAREA;
            obj.UPDATED_AT = moment().format('YYYY-MM-DD HH:mm:ss');

            data.push(obj);
          });
        }

        q.resolve(data);

      }

    });

    return q.promise;

  },

  laborGetPerson: function (db) {

    var q = Q.defer();
    var col = db.collection('anc');

    col.aggregate([
      {
        $group: {
          _id: {
            HOSPCODE: "$HOSPCODE",
            PID: "$PID"
          }
        }
      }
    ], function (err, docs) {
      if (err) {
        q.reject(err);
      } else {
        var data = [];
        if (docs) {
          _.forEach(docs, function (v) {
            var npid = v._id.HOSPCODE + v._id.PID;
            data.push(npid);
          });
        }

        q.resolve(data);

      }

    });

    return q.promise;

  }
};