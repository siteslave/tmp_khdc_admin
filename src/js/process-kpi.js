$(function () {

  var MongoClient = require('mongodb').MongoClient;
  var process = require('../models/process');
  var config = require('../libs/config');
  var ProcessMySQL = require('../models/process_mysql');

  var configDb = config.getDBConnection();
  /*
   hdc: {
   host: 'localhost',
   database: 'khdc',
   port: 1271,
   user: 'khdc',
   password: 'khdc'
   }
   */

  $('#checkAll').on('click', function (e) {
    $('input:checkbox[data-name="chkProcess"]').prop('checked', $(this).prop('checked'));
  });

  $('#btnProcess').on('click', function (e) {
    e.preventDefault();
    //var data = [];
    //$('input:checkbox[data-name="chkProcess"]').each(function (e) {
    //  if ($(this).prop('checked')) {
    //    data.push($(this).data('id'));
    //  }
    //});

    // var mongoConnect = MongoClient.connect('mongodb://127.0.0.1:27017/khdc');

    var dialog = $('#dialog').data('dialog');
    dialog.open();

    setTimeout(function () {
      // MySQL Connection
      var knex = require('knex')({
        client: 'mysql',
        connection : {
          host: configDb.mysql.host,
          port: parseInt(configDb.mysql.port),
          database: configDb.mysql.database,
          user: configDb.mysql.user,
          password: configDb.mysql.password
        }
      });

      var url = 'mongodb://' + configDb.mongo.host + ':' + parseInt(configDb.mongo.port) + '/' + configDb.mongo.database;

      MongoClient.connect(url, function(err, db) {
        if (err) console.log(err);
        else {
          process.personPyramid(db)
            .then(function (data) {
              return ProcessMySQL.savePersonPyramid(knex, data);
            })
            .then(function () {
              return process.personSummary(db);
            })
            .then(function (data) {
              return ProcessMySQL.savePersonSummary(knex, data);
            })
            .then(function () {
              return process.serviceSummary(db);
            })
            .then(function (data) {
              return ProcessMySQL.saveServiceSummary(knex, data);
            })
            .then(function () {
              return process.diagOpdSummary(db);
            })
            .then(function (data) {
              return ProcessMySQL.saveDiagOpdSummary(knex, data);
            })
            .then(function () {
              return process.laborGetPerson(db);
            })
            .then(function (pids) {
              return process.laborTransformPerson(db, pids);
            })
            .then(function (data) {
              return ProcessMySQL.saveLaborTransformPerson(knex, data);
            })
            .then(function () {
              dialog.close();
              $.Notify({
                caption: 'การประมวลผล',
                content: 'ประมวลผลข้อมูลเสร็จเรียบร้อยแล้ว',
                type: 'success'
              });
            }, function (err) {
              console.log(err);
              dialog.close();
              $.Notify({
                caption: 'การประมวลผล',
                content: 'เกิดข้อผิดพลาดในการประมวลผล กรุณาตรวจสอบ Log',
                type: 'alert'
              });
            });
        }
      });
    }, 2000);
  });

});