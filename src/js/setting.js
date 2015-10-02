$(function () {
  var ipc = require('ipc');
  var fse = require('fs-extra');

  var configFile = ipc.sendSync('get-config-file');
  //console.log(configFile);
  //
  var getConfig = function() {

    var config = fse.readJsonSync(configFile);

    var mongo = config.mongo;
    var mysql = config.mysql;

    console.log(config);

    $('#txtMongoDBHost').val(mongo.host);
    $('#txtMongoDBPort').val(mongo.port);
    $('#txtMongoDBDatabase').val(mongo.database);
    $('#txtMongoDBUsername').val(mongo.user);
    $('#txtMongoDBPassword').val(mongo.password);

    $('#txtMySQLHost').val(mysql.host);
    $('#txtMySQLPort').val(mysql.port);
    $('#txtMySQLDatabase').val(mysql.database);
    $('#txtMySQLUsername').val(mysql.user);
    $('#txtMySQLPassword').val(mysql.password);
  };

  getConfig();

  $('#btnSave').on('click', function(e) {
    e.preventDefault();

    var config = {};
    config.mongo = {};
    config.mongo.host = $('#txtMongoDBHost').val();
    config.mongo.port = $('#txtMongoDBPort').val();
    config.mongo.database = $('#txtMongoDBDatabase').val();
    config.mongo.user = $('#txtMongoDBUsername').val();
    config.mongo.password = $('#txtMongoDBPassword').val();

    config.mysql = {};
    config.mysql.host = $('#txtMySQLHost').val();
    config.mysql.port = $('#txtMySQLPort').val();
    config.mysql.database = $('#txtMySQLDatabase').val();
    config.mysql.user = $('#txtMySQLUsername').val();
    config.mysql.password = $('#txtMySQLPassword').val();

    if (!config.mongo.host && !config.mongo.port && !config.mongo.database && !config.mongo.user && !config.mongo.password &&
      !config.mysql.host && !config.mysql.port && !config.mysql.database && !config.mysql.user && !config.mysql.password) {

      $.Notify({
        caption: 'Alert',
        content: 'Error: Data invalid',
        type: 'error'
      });

    } else {
      fse.writeJson(configFile, config, function(err) {
        if (err) {
          console.log(err);
          $.Notify({
            caption: 'Alert',
            content: 'Error: Can\'t save config',
            type: 'error'
          });
        } else {
          $.Notify({
            caption: 'Alert',
            content: 'Save successfully',
            type: 'success'
          });
        }
      });
    }
  });

});