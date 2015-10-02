$(function() {
  var ipc = require('ipc');
  var fs = require('fs');
  var fse = require('fs-extra');
  var path = require('path');
  var filesize = require('file-size');
  var moment = require('moment');
  var shell = require('shell');
  var Finder = require('fs-finder');
  var rimraf = require('rimraf');
  var _ = require('lodash');

  var Q = require('q');
  require('q-foreach')(Q);

  var Importor = require('../models/import');

  var appPath = ipc.sendSync('get-app-path');
  var inboxPath = ipc.sendSync('get-inbox-path');
  var extractedPath = path.join(appPath, 'extracted');

  var config = require('../libs/config');

  var configDb = config.getDBConnection();


  $('#bntOpenExportFolder').on('click', function(e) {
    e.preventDefault();
    shell.showItemInFolder(inboxPath);
  });

  var getWaitingList = function () {
    var $table = $('#tblList > tbody');
    $table.empty();

    var files = Finder.from(inboxPath).findFiles();
    files.forEach(function(file) {
      //var file = path.join(inboxPath, f);
      if (path.extname(file).toUpperCase() == ".ZIP") {
        fs.stat(file, function(err, stats) {
          if (err) {
            $.Notify({
              caption: 'Alert',
              content: 'Can\'t get file list',
              type: 'error'
            });
          } else {
            var _file = {};
            _file.size = filesize(stats.size).human('si');
            _file.created_time = moment(stats.mtime).format('DD/MM/YYYY HH:mm:ss');
            _file.name = path.basename(file);
            _file.path = file;
            var tmpl = $.templates('#fileTemplate');
            var html = tmpl.render(_file);
            $table.append(html);
          }
        });
      }
    });

  };

  getWaitingList();

  $('#btnRefresh').on('click', function (e) {
    e.preventDefault();

    getWaitingList();
  });

  var doImportFile = function (filePath) {
    var q = Q.defer();
    // do import
    var _extractDirectory = path.join(extractedPath, moment().format('x'));
    // create extracted directory
    fse.ensureDirSync(_extractDirectory);
    //console.log(docs.file_path);
    Importor.doExtract(filePath, _extractDirectory)
      .then(function () {
        // Get file list
        var _files = Finder.from(_extractDirectory).findFiles('*.txt');
        var _objFiles = {};

        if (_.size(_files)) {
          _.forEach(_files, function (file) {
            var fileName = path.basename(file).toUpperCase();

            if (fileName == 'ACCIDENT.TXT') _objFiles.accident = file;
            if (fileName == 'ADDRESS.TXT') _objFiles.address = file;
            if (fileName == 'ADMISSION.TXT') _objFiles.admission = file;
            if (fileName == 'ANC.TXT') _objFiles.anc = file;
            if (fileName == 'APPOINTMENT.TXT') _objFiles.appointment = file;
            if (fileName == 'CARD.TXT') _objFiles.card = file;
            if (fileName == 'CARE_REFER.TXT') _objFiles.care_refer = file;
            if (fileName == 'CHARGE_IPD.TXT') _objFiles.charge_ipd = file;
            if (fileName == 'CHARGE_OPD.TXT') _objFiles.charge_opd = file;
            if (fileName == 'CHRONIC.TXT') _objFiles.chronic = file;
            if (fileName == 'CHRONICFU.TXT') _objFiles.chronicfu = file;
            if (fileName == 'CLINICAL_REFER.TXT') _objFiles.clinical_refer = file;
            if (fileName == 'COMMUNITY_ACTIVITY.TXT') _objFiles.community_activity = file;
            if (fileName == 'COMMUNITY_SERVICE.TXT') _objFiles.community_service = file;
            if (fileName == 'DEATH.TXT') _objFiles.death = file;
            if (fileName == 'DENTAL.TXT') _objFiles.dental = file;
            if (fileName == 'DIAGNOSIS_IPD.TXT') _objFiles.diagnosis_ipd = file;
            if (fileName == 'DIAGNOSIS_OPD.TXT') _objFiles.diagnosis_opd = file;
            if (fileName == 'DISABILITY.TXT') _objFiles.disability = file;
            if (fileName == 'DRUG_IPD.TXT') _objFiles.drug_ipd = file;
            if (fileName == 'DRUG_OPD.TXT') _objFiles.drug_opd = file;
            if (fileName == 'DRUG_REFER.TXT') _objFiles.drug_refer = file;
            if (fileName == 'DRUGALLERGY.TXT') _objFiles.drugallergy = file;
            if (fileName == 'EPI.TXT') _objFiles.epi = file;
            if (fileName == 'FP.TXT') _objFiles.fp = file;
            if (fileName == 'FUNCTIONAL.TXT') _objFiles.functional = file;
            if (fileName == 'HOME.TXT') _objFiles.home = file;
            if (fileName == 'ICF.TXT') _objFiles.icf = file;
            if (fileName == 'INVESTIGATION_REFER.TXT') _objFiles.investigation_refer = file;
            if (fileName == 'LABFU.TXT') _objFiles.labfu = file;
            if (fileName == 'LABOR.TXT') _objFiles.labor = file;
            if (fileName == 'NCDSCREEN.TXT') _objFiles.ncdscreen = file;
            if (fileName == 'NEWBORN.TXT') _objFiles.newborn = file;
            if (fileName == 'NEWBORNCARE.TXT') _objFiles.newborncare = file;
            if (fileName == 'NUTRITION.TXT') _objFiles.nutrition = file;
            if (fileName == 'PERSON.TXT') _objFiles.person = file;
            if (fileName == 'POSTNATAL.TXT') _objFiles.postnatal = file;
            if (fileName == 'PRENATAL.TXT') _objFiles.prenatal = file;
            if (fileName == 'PROCEDURE_IPD.TXT') _objFiles.procedure_ipd = file;
            if (fileName == 'PROCEDURE_OPD.TXT') _objFiles.procedure_opd = file;
            if (fileName == 'PROCEDURE_REFER.TXT') _objFiles.procedure_refer = file;
            if (fileName == 'PROVIDER.TXT') _objFiles.provider = file;
            if (fileName == 'REFER_HISTORY.TXT') _objFiles.refer_history = file;
            if (fileName == 'REFER_RESULT.TXT') _objFiles.refer_result = file;
            if (fileName == 'REHABILITATION.TXT') _objFiles.rehabilitation = file;
            if (fileName == 'SERVICE.TXT') _objFiles.service = file;
            if (fileName == 'SPECIALPP.TXT') _objFiles.specialpp = file;
            if (fileName == 'SURVEILLANCE.TXT') _objFiles.surveillance = file;
            if (fileName == 'VILLAGE.TXT') _objFiles.village = file;
            if (fileName == 'WOMEN.TXT') _objFiles.women = file;
          });

          var MongoClient = require('mongodb').MongoClient;
          // var mongoConnect = MongoClient.connect('mongodb://127.0.0.1:27017/khdc');

          var url = 'mongodb://' + configDb.mongo.host + ':' + parseInt(configDb.mongo.port) + '/' + configDb.mongo.database;

          MongoClient.connect(url, function(err, db) {
            if (err) q.reject(err);
            else {
              if (_objFiles.person) {
                Importor.toJson(_objFiles.person)
                  .then(function (data) {
                    return Importor.importPerson(db, data);
                  })
                  .then(function () {
                    return _objFiles.accident ? Importor.toJson(_objFiles.accident) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importAccident(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.address ? Importor.toJson(_objFiles.address) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importAddress(db, data) : null;
                  })
                  .then(function () {
                    return Importor.toJson(_objFiles.admission);
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importAdmission(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.anc ? Importor.toJson(_objFiles.anc) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importAnc(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.appointment ? Importor.toJson(_objFiles.appointment) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importAppointment(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.card ? Importor.toJson(_objFiles.card) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importCard(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.care_refer ? Importor.toJson(_objFiles.care_refer) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importCareRefer(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.charge_ipd ? Importor.toJson(_objFiles.charge_ipd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importChargeIpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.charge_opd ? Importor.toJson(_objFiles.charge_opd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importChargeOpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.chronic ? Importor.toJson(_objFiles.chronic) : [];
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importChronic(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.chronicfu ? Importor.toJson(_objFiles.chronicfu) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importChronicFu(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.clinical_refer ? Importor.toJson(_objFiles.clinical_refer) : [];
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importClinicalRefer(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.community_activity ? Importor.toJson(_objFiles.community_activity) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importCommunityActivity(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.community_service ? Importor.toJson(_objFiles.community_service) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importCommunityService(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.death ? Importor.toJson(_objFiles.death) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDeath(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.dental ? Importor.toJson(_objFiles.dental) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDental(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.diagnosis_ipd ? Importor.toJson(_objFiles.diagnosis_ipd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDiagnosisIpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.diagnosis_opd ? Importor.toJson(_objFiles.diagnosis_opd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDiagnosisOpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.disability ? Importor.toJson(_objFiles.disability) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDisability(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.drug_ipd ? Importor.toJson(_objFiles.drug_ipd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDrugIpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.drug_opd ? Importor.toJson(_objFiles.drug_opd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDrugOpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.drug_refer ? Importor.toJson(_objFiles.drug_refer) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDrugRefer(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.drugallergy ? Importor.toJson(_objFiles.drugallergy) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importDrugAllergy(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.epi ? Importor.toJson(_objFiles.epi) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importEpi(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.fp ? Importor.toJson(_objFiles.fp) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importFp(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.functional ? Importor.toJson(_objFiles.functional) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importFunctional(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.home ? Importor.toJson(_objFiles.home) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importHome(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.icf ? Importor.toJson(_objFiles.icf) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importIcf(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.investigation_refer ? Importor.toJson(_objFiles.investigation_refer) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importInvestigationRefer(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.labfu ? Importor.toJson(_objFiles.labfu) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importLabfu(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.labor ? Importor.toJson(_objFiles.labor) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importLabor(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.ncdscreen ? Importor.toJson(_objFiles.ncdscreen) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importNcdScreen(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.newborn ? Importor.toJson(_objFiles.newborn) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importNewborn(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.newborncare ? Importor.toJson(_objFiles.newborncare) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importNewbornCare(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.nutrition ? Importor.toJson(_objFiles.nutrition) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importNutrition(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.postnatal ? Importor.toJson(_objFiles.postnatal) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importPostnatal(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.prenatal ? Importor.toJson(_objFiles.prenatal) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importPrenatal(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.procedure_ipd ? Importor.toJson(_objFiles.procedure_ipd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importProcedureIpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.procedure_opd ? Importor.toJson(_objFiles.procedure_opd) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importProcedureOpd(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.procedure_refer ? Importor.toJson(_objFiles.procedure_refer) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importProcedureRefer(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.provider ? Importor.toJson(_objFiles.provider) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importProvider(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.refer_history ? Importor.toJson(_objFiles.refer_history) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importReferHistory(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.refer_result ? Importor.toJson(_objFiles.refer_result) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importReferResult(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.rehabilitation ? Importor.toJson(_objFiles.rehabilitation) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importRehabilitation(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.service ? Importor.toJson(_objFiles.service) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importService(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.specialpp ? Importor.toJson(_objFiles.specialpp) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importSpecialPP(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.surveillance ? Importor.toJson(_objFiles.surveillance) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importSurveillance(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.village ? Importor.toJson(_objFiles.village) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importVillage(db, data) : null;
                  })
                  .then(function () {
                    return _objFiles.women ? Importor.toJson(_objFiles.women) : null;
                  })
                  .then(function (data) {
                    return _.size(data) ? Importor.importWomen(db, data) : null;
                  })
                  .then(function () {
                    rimraf.sync(_extractDirectory);
                    rimraf.sync(filePath);
                    db.close();
                    q.resolve();
                  }, function (err) {
                    db.close();
                    q.reject('Can\'t import file: ' + filePath);
                    $.Notify({
                      caption: 'Alert',
                      content: 'Can\'t import file: ' + filePath + '\n' + JSON.stringify(err),
                      type: 'alert',
                      keepOpen: true
                    });

                    console.log(err);
                    var dialog = $('#dialog2').data('dialog');
                    dialog.close();

                  });
              } else {
                q.reject('File PERSON.txt not found!: ' + filePath);
              }
            }
          });

        } else {
          q.reject('File not found!');
        }
      });

    return q.promise;
  };


  $(document).on('click', 'a[data-name="btnImport"]', function(e) {
    e.preventDefault();
    var $that = $(this).parent().parent().parent().parent().parent();

    if (confirm('คุณต้องการที่จะนำเข้าไฟล์นี้ ใช่หรือไม่?')) {

      var dialog = $('#dialog').data('dialog');
      dialog.open();
      //var file = $(this).data('path');
      var filePath = $(this).data('path');
      doImportFile(filePath)
        .then(function () {
          dialog.close();
          $.Notify({
            caption: 'Alert',
            content: 'Import file success',
            type: 'success'
          });

          $that.fadeOut('slow');
          //getWaitingList();
        }, function (err) {
          console.log(err);
          $.Notify({
            caption: 'Alert',
            content: 'Import file failed!',
            type: 'alert'
          });
          dialog.close();
        });

    }

  });

  $('#btnImportAll').on('click', function(e) {
    e.preventDefault();
    //var $that = $(this).parent().parent().parent().parent().parent();
    $('#txtTotal').text('0');
    $('#txtCurrent').text('0');
    $('#txtCurrentFile').text('');
    var $pb = $("#progress").data('progress');

    if (confirm('คุณต้องการที่จะนำเข้าไฟล์นี้ ใช่หรือไม่?')) {

      var dialog = $('#dialog2').data('dialog');
      dialog.open();

      var files = Finder.from(inboxPath).findFiles();
      var totalFile = files.length;

      if (totalFile) {
        $('#txtTotal').text(files.length);
        var i = 0;
        Q.forEach(files, function(file) {
          var q = Q.defer();
          //var file = path.join(inboxPath, f);
          i++;

          if (path.extname(file).toUpperCase() == ".ZIP") {
            $('#txtCurrentFile').text(path.basename(file).toUpperCase());
            doImportFile(file)
              .then(function () {
                q.resolve();
                $('#txtCurrent').text(i);
                var current = (i * 100) / totalFile;
                $pb.set(current);

                if (i == files.length) {
                  $.Notify({
                    caption: 'Alert',
                    content: 'Import file success',
                    type: 'success'
                  });

                  setTimeout(function () {
                    dialog.close();
                  }, 2500);

                  getWaitingList();
                }
              }, function (err) {
                console.log(JSON.stringify(err));
                $.Notify({
                  caption: 'Alert',
                  content: 'Can\'t import file: ' + file,
                  type: 'alert'
                });
                dialog.close();
                q.reject('Can\'t import file: ' + file);
              });
          } else {
            q.reject('File not found!: ' + file);
          }

          return q.promise;
        });
      } else {
        dialog.close();
        $.Notify({
          caption: 'Alert',
          content: 'ไม่พบไฟล์ที่ต้องการนำเข้า กรุณาตรวจสอบ',
          type: 'alert'
        });
      }

    }

  });

  // Remove file
  $(document).on('click', 'a[data-name="btnRemove"]', function(e) {
    e.preventDefault();

    var filePath = $(this).data('path');

    //console.log(filePath);
    if (confirm('Are sure?')) {
      fse.remove(filePath, function(err) {
        if (err) {
          $.Notify({
            caption: 'Alert',
            content: 'Can\'t remove file',
            type: 'alert'
          });
        } else {
          getWaitingList();
          $.Notify({
            caption: 'Alert',
            content: 'Remove file successfully',
            type: 'success'
          });
        }
      });
    }
  });

});