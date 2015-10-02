var fse = require('fs-extra');
var Q = require('q');
var ipc = require('ipc');

module.exports = {
  getDBConnection: function () {
    var configFile = ipc.sendSync('get-config-file');
    return fse.readJsonSync(configFile);
  }
};