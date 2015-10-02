var Q = require('q');
var fs = require('fs');
var fse = require('fs-extra');
var path = require('path');
var Zip = require('jszip');
var _ = require('lodash');
var moment = require('moment');
var parse = require('csv-parse');

module.exports = {
  doExtract: function (zipFile, extractPath) {
    var q = Q.defer();

    fs.readFile(zipFile, function (err, data) {
      if (err) q.reject(err);
      else {
        zip = new Zip();
        zip.folder(extractPath).load(data);

        Object.keys(zip.files).forEach(function (filename) {
          //console.log(filename);
          if(path.extname(filename) == '.txt') {
            var content = zip.files[filename].asNodeBuffer();
            fse.outputFileSync(filename, content);
          }
        });
        q.resolve();
      }
    });

    return q.promise;
  },

  toJson: function (csvFile) {
    var q = Q.defer();

    fs.readFile(csvFile, 'utf8', function (err, data) {
      if (err) {
        q.reject(err);
      } else {

        parse(data, {delimiter: '|', columns: true}, function (err, output) {
            if (err) {
              q.reject(err);
            } else {
              q.resolve(output);
            }
        });
      }
    });

    return q.promise;
  },

  importService: function (db, data) {
    var q = Q.defer();
    var col = db.collection('service');

    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});
    var total = 0;
    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.DATE_SERV) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.PRICE = parseFloat(v.PRICE);
        v.COST = parseFloat(v.COST);
        v.PAYPRICE = parseFloat(v.PAYPRICE);
        v.ACTUALPAY = parseFloat(v.ACTUALPAY);

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, SEQ: v.SEQ})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importAccident: function (db, data) {
    var q = Q.defer();

    var col = db.collection('accident');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;
    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.DATETIME_SERV && v.DATETIME_AE && v.AETYPE) {
        total++;
        v.DATETIME_SERV = new Date(moment(v.DATETIME_SERV, "YYYYMMDDHHmmss").format());
        v.DATETIME_AE = new Date(moment(v.DATETIME_AE, "YYYYMMDDHHmmss").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, SEQ: v.SEQ})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importAddress: function (db, data) {
    var q = Q.defer();
    var col = db.collection('address');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;
    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importAdmission: function (db, data) {
    var q = Q.defer();
    var col = db.collection('addmission');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATETIME_ADMIT && v.HOSPCODE && v.PID && v.AN) {
        total++;
        v.DATETIME_ADMIT = new Date(moment(v.DATETIME_ADMIT, "YYYYMMDDHHmmss").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, AN: v.AN})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importAnc: function (db, data) {
    var q = Q.defer();    
    var col = db.collection('anc');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;
    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.DATE_SERV) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, DATE_SERV: v.DATE_SERV})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importAppointment: function (db, data) {
    var q = Q.defer();
    var col = db.collection('appointment');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.APTYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, SEQ: v.SEQ, APTYPE: v.APTYPE})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importCard: function (db, data) {
    var q = Q.defer();
    var col = db.collection('card');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.INSTYPE_NEW) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, PID: v.PID, INSTYPE_NEW: v.INSTYPE_NEW})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importCareRefer: function (db, data) {
    var q = Q.defer();
    var col = db.collection('care_refer');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.CARETYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({HOSPCODE: v.HOSPCODE, REFERID: v.REFERID, CARETYPE: v.CARETYPE})
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importChargeIpd: function (db, data) {
    var q = Q.defer();
    var col = db.collection('charege_ipd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATETIME_ADMIT && v.HOSPCODE && v.PID && v.AN && v.CHARGEITEM && v.CHARGELIST && v.INSTYPE) {
        total++;
        v.DATETIME_ADMIT = new Date(moment(v.DATETIME_ADMIT, "YYYYMMDDHHmmss").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.PRICE = parseFloat(v.PRICE);
        v.COST = parseFloat(v.COST);
        v.QUANTITY = parseInt(v.QUANTITY);
        v.PAYPRICE = parseFloat(v.PAYPRICE);

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          AN: v.AN,
          DATETIME_ADMIT: v.DATETIME_ADMIT,
          CHARGEITEM: v.CHARGEITEM,
          CHARGELIST: v.CHARGELIST,
          INSTYPE: v.INSTYPE
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importChargeOpd: function (db, data) {
    var q = Q.defer();
    var col = db.collection('charge_opd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.CHARGEITEM && v.CHARGELIST && v.INSTYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.PRICE = parseFloat(v.PRICE);
        v.COST = parseFloat(v.COST);
        v.QUANTITY = parseInt(v.QUANTITY);
        v.PAYPRICE = parseFloat(v.PAYPRICE);

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          CHARGEITEM: v.CHARGEITEM,
          CHARGELIST: v.CHARGELIST,
          INSTYPE: v.INSTYPE
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importChronic: function (db, data) {
    var q = Q.defer();
    var col = db.collection('chronic');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_DIAG && v.HOSPCODE && v.PID && v.CHRONIC) {
        total++;
        v.DATE_DIAG = new Date(moment(v.DATE_DIAG, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          CHRONIC: v.CHRONIC
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importChronicFu: function (db, data) {
    var q = Q.defer();
    var col = db.collection('chronic_fu');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importClinicalRefer: function (db, data) {
    var q = Q.defer();

    var col = db.collection('clinical_refer');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.DATETIME_ASSESS && v.CLINICALCODE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATETIME_ASSESS = new Date(moment(v.DATETIME_ASSESS, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID,
          DATETIME_ASSESS: v.DATETIME_ASSESS,
          CLINICALCODE: v.CLINICALCODE
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importCommunityActivity: function (db, data) {
    var q = Q.defer();
    var col = db.collection('community_activity');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.VID && v.DATE_START && v.COMACTIVITY) {
        total++;

        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_START = new Date(moment(v.DATE_START, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          VID: v.VID,
          DATE_START: v.DATE_START,
          COMACTIVITY: v.COMACTIVITY
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importCommunityService: function (db, data) {
    var q = Q.defer();

    var col = db.collection('community_service');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ && v.COMSERVICE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          COMSERVICE: v.COMSERVICE
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDeath: function (db, data) {
    var q = Q.defer();

    var col = db.collection('death');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DDEATH && v.HOSPCODE && v.PID) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DDEATH = new Date(moment(v.DDEATH, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDental: function (db, data) {
    var q = Q.defer();

    var col = db.collection('dental');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDiagnosisIpd: function (db, data) {
    var q = Q.defer();

    var col = db.collection('diagnosis_ipd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.AN && v.DIAGCODE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATETIME_ADMIT = new Date(moment(v.DATETIME_ADMIT, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          AN: v.AN,
          DIAGCODE: v.DIAGCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDiagnosisOpd: function (db, data) {
    var q = Q.defer();

    var col = db.collection('diagnosis_opd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.DIAGCODE && v.DATE_SERV) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          DIAGCODE: v.DIAGCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDisability: function (db, data) {
    var q = Q.defer();

    var col = db.collection('disability');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.DISABTYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DISABTYPE: v.DISABTYPE
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDrugIpd: function (db, data) {
    var q = Q.defer();

    var col = db.collection('drug_ipd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.AN && v.TYPEDRUG && v.DIDSTD) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATETIME_ADMIT = new Date(moment(v.DATETIME_ADMIT, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          AN: v.AN,
          TYPEDRUG: v.TYPEDRUG,
          DIDSTD: v.DIDSTD
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDrugOpd: function (db, data) {
    var q = Q.defer();

    var col = db.collection('drug_opd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ && v.DIDSTD) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          DIDSTD: v.DIDSTD
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDrugRefer: function (db, data) {
    var q = Q.defer();

    var col = db.collection('drug_refer');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.DATETIME_DSTART && v.DIDSTD) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATETIME_DSTART = new Date(moment(v.DATETIME_DSTART, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID,
          DATETIME_DSTART: v.DATETIME_DSTART,
          DIDSTD: v.DIDSTD
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importDrugAllergy: function (db, data) {
    var q = Q.defer();

    var col = db.collection('drug_allergy');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.DRUGALLERGY) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DRUGALLERGY: v.DRUGALLERGY
        })
          .upsert().updateOne(v);
      }

    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importEpi: function (db, data) {
    var q = Q.defer();

    var col = db.collection('epi');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.DATE_SERV && v.VACCINETYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV,
          VACCINETYPE: v.VACCINETYPE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importFp: function (db, data) {
    var q = Q.defer();

    var col = db.collection('fp');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.FPTYPE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV,
          FPTYPE: v.FPTYPE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importFunctional: function (db, data) {
    var q = Q.defer();

    var col = db.collection('functional');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ && v.FUNCTIONAL_TEST) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          FUNCTIONAL_TEST: v.FUNCTIONAL_TEST
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importHome: function (db, data) {
    var q = Q.defer();

    var col = db.collection('home');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.HID) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          HID: v.HID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importIcf: function (db, data) {
    var q = Q.defer();

    var col = db.collection('icf');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.SEQ && v.ICF) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          ICF: v.ICF
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importInvestigationRefer: function (db, data) {
    var q = Q.defer();

    var col = db.collection('investigation_refer');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATETIME_INVEST && v.HOSPCODE && v.REFERID && v.INVESTCODE) {
        total++;
        v.DATETIME_INVEST = new Date(moment(v.DATETIME_INVEST, "YYYYMMDDHHmmss").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID,
          DATETIME_INVEST: v.DATETIME_INVEST,
          INVESTCODE: v.INVESTCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importLabfu: function (db, data) {
    var q = Q.defer();

    var col = db.collection('labfu');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.LABTEST && v.DATE_SERV) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          LABTEST: v.LABTEST
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importLabor: function (db, data) {
    var q = Q.defer();

    var col = db.collection('labor');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.BDATE && v.HOSPCODE && v.PID && v.GRAVIDA) {
        total++;
        v.BDATE = new Date(moment(v.BDATE, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          GRAVIDA: v.GRAVIDA
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importNcdScreen: function (db, data) {
    var q = Q.defer();

    var col = db.collection('ncdscreen');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importNewborn: function (db, data) {
    var q = Q.defer();

    var col = db.collection('newborn');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.BDATE && v.HOSPCODE && v.PID && v.GRAVIDA) {
        total++;
        v.BDATE = new Date(moment(v.BDATE, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importNewbornCare: function (db, data) {
    var q = Q.defer();

    var col = db.collection('newborn_care');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.BCARE && v.HOSPCODE && v.PID) {
        total++;
        v.BCARE = new Date(moment(v.BCARE, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          BCARE: v.BCARE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importNutrition: function (db, data) {
    var q = Q.defer();

    var col = db.collection('nutrition');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importPerson: function (db, data) {
    var q = Q.defer();

    var col = db.collection('person');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.BIRTH && v.HOSPCODE && v.PID) {
        total++;
        v.BIRTH = new Date(moment(v.BIRTH, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importPostnatal: function (db, data) {
    var q = Q.defer();

    var col = db.collection('postnatal');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.GRAVIDA && v.PPCARE) {
        total++;
        v.PPCARE = new Date(moment(v.PPCARE, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          GRAVIDA: v.GRAVIDA,
          PPCARE: v.PPCARE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importPrenatal: function (db, data) {
    var q = Q.defer();

    var col = db.collection('prenatal');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.GRAVIDA && v.LMP && v.EDC) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.LMP = new Date(moment(v.LMP, "YYYYMMDD").format());
        v.EDC = new Date(moment(v.EDC, "YYYYMMDD").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          GRAVIDA: v.GRAVIDA
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importProcedureIpd: function (db, data) {
    var q = Q.defer();

    var col = db.collection('procedure_ipd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATETIME_ADMIT && v.HOSPCODE && v.PID && v.AN && v.PROCEDCODE && v.TIMESTART) {
        total++;
        v.DATETIME_ADMIT = new Date(moment(v.DATETIME_ADMIT, "YYYYMMDDHHmmss").format());
        v.TIMESTART = new Date(moment(v.TIMESTART, "YYYYMMDDHHmmss").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          AN: v.AN,
          PROCEDCODE: v.PROCEDCODE,
          TIMESTART: v.TIMESTART
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importProcedureOpd: function (db, data) {
    var q = Q.defer();
    var col = db.collection('procedure_opd');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.SEQ && v.PID && v.PROCEDCODE) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          PROCEDCODE: v.PROCEDCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }


    return q.promise;
  },

  importProcedureRefer: function (db, data) {
    var q = Q.defer();
    var col = db.collection('procedure_refer');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.TIMESTART && v.PROCEDCODE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.TIMESTART = new Date(moment(v.TIMESTART, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID,
          TIMESTART: v.TIMESTART,
          PROCEDCODE: v.PROCEDCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importProvider: function (db, data) {
    var q = Q.defer();    

    var col = db.collection('provider');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PROVIDER) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PROVIDER: v.PROVIDER
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importReferHistory: function (db, data) {
    var q = Q.defer();

    var col = db.collection('refer_history');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.DATETIME_SERV) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());
        v.DATETIME_SERV = new Date(moment(v.DATETIME_SERV, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importReferResult: function (db, data) {
    var q = Q.defer();

    var col = db.collection('refer_result');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.REFERID && v.HOSP_SOURCE) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          REFERID: v.REFERID,
          HOSP_SOURCE: v.HOSP_SOURCE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importRehabilitation: function (db, data) {
    var q = Q.defer();

    var col = db.collection('rehabilitation');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.DATE_SERV && v.HOSPCODE && v.PID && v.REHABCODE) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV,
          REHABCODE: v.REHABCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importSpecialPP: function (db, data) {
    var q = Q.defer();

    var col = db.collection('special_pp');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.DATE_SERV && v.PPSPECIAL) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          DATE_SERV: v.DATE_SERV,
          PPSPECIAL: v.PPSPECIAL
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importSurveillance: function (db, data) {
    var q = Q.defer();

    var col = db.collection('surveillance');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID && v.SEQ && v.DIAGCODE && v.DATE_SERV) {
        total++;
        v.DATE_SERV = new Date(moment(v.DATE_SERV, "YYYYMMDD").format());
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID,
          SEQ: v.SEQ,
          DIAGCODE: v.DIAGCODE
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importVillage: function (db, data) {
    var q = Q.defer();

    var col = db.collection('village');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.VID) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          VID: v.VID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  },

  importWomen: function (db, data) {
    var q = Q.defer();

    var col = db.collection('women');
    var query = col.initializeUnorderedBulkOp({useLegacyOps: true});

    var total = 0;

    _.forEach(data, function (v) {
      if (v.HOSPCODE && v.PID) {
        total++;
        v.D_UPDATE = new Date(moment(v.D_UPDATE, "YYYYMMDDHHmmss").format());

        query.find({
          HOSPCODE: v.HOSPCODE,
          PID: v.PID
        })
          .upsert().updateOne(v);
      }
    });

    if (total) {
      query.execute(function (err, res) {
        if (err) {
          q.reject(err);
        } else {
          q.resolve();
        }
      });
    } else {
      q.resolve();
    }

    return q.promise;
  }
  
};