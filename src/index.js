var app = require('app');  // Module to control application life.
var BrowserWindow = require('browser-window');  // Module to create native browser window.
var ipc = require('ipc');
var fse = require('fs-extra');
var path = require('path');
var fs = require('fs');

// Report crashes to our server.
// require('crash-reporter').start();

// Keep a global reference of the window object, if you don't, the window will
// be closed automatically when the javascript object is GCed.
var mainWindow = null;

// Quit when all windows are closed.
app.on('window-all-closed', function() {
    app.quit();
});

// This method will be called when Electron has done everything
// initialization and ready for creating browser windows.
app.on('ready', function() {
    var dataPath = app.getPath('appData');
    var appPath = path.join(dataPath, 'khdc');
    var inboxPath = path.join(appPath, 'inbox');
    var configFile = path.join(appPath, 'config.json');

    fse.ensureDirSync(appPath);
    fse.ensureDirSync(inboxPath);

    fs.access(configFile, fs.W_OK && fs.R_OK, function(err) {
        if (err) {
            var defaultConfig = {
                mongo: {
                    host: 'localhost',
                    database: 'khdc',
                    port: 27017,
                    user: 'khdc',
                    password: 'khdc'
                },
                mysql: {
                    host: 'localhost',
                    database: 'khdc',
                    port: 3306,
                    user: 'khdc',
                    password: 'khdc'
                }
            };

            fse.writeJsonSync(configFile, defaultConfig);
        }
    });

    // ipc modules
    ipc.on('get-app-path', function(event, arg) {
        event.returnValue = appPath;
    });
    
    ipc.on('get-config-file', function(event, arg) {
        event.returnValue = configFile;
    });

    ipc.on('get-inbox-path', function(event, arg) {
        event.returnValue = inboxPath;
    });

    ipc.on('show-select-directory', function(event, arg) {
        var dialog = require('dialog');
        var dir = dialog.showOpenDialog({ properties: ['openDirectory']});
        event.returnValue = dir;
    });
    // Create the browser window.
    mainWindow = new BrowserWindow({width: 1010, height: 600});

    // and load the index.html of the app.
    mainWindow.loadUrl('file://' + __dirname + '/pages/index.html');

    // Emitted when the window is closed.
    mainWindow.on('closed', function() {
        // Dereference the window object, usually you would store windows
        // in an array if your app supports multi windows, this is the time
        // when you should delete the corresponding element.
        mainWindow = null;
    });
});