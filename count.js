var mysql = require('mysql');
var async = require('async');

var connectionOptions = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT
};

if (process.env.DB_SSL) {
  // SSL is used for Amazon RDS, but not necessarily for local dev
  connectionOptions.ssl = process.env.DB_SSL;
}

var contributionTypes = [
  'is_adhoc',
  'is_badge',
  'is_event_host',
  'is_event_mentor',
  'is_event_coorganizer',
  'is_github_code',
  'is_github_issue',
  'is_mozfest_contributor',
  'is_bugzilla_bug',
  'is_bugzilla_comment',
  'is_transifex',
];

/**
 * getCountValue
 * @param  {string}   sql      A query that returns a single field named `count`
 */
function getCountValue(sql, values, callback) {
  var connection = mysql.createConnection(connectionOptions);
  connection.connect(function connectionAttempted(err) {
    if (err) {
      console.error(err);
      callback(err);
    } else {

      connection.query(sql, values, function (err, result) {
        if (err) {
          console.error(err);
          callback(err);
        }
        connection.end();
        callback(null, result[0].count);
      });
    }
  });
}


function getCountForTwoTypes (type1, type2, callback) {
  if (type1 === type2) {
    return callback();
  }

  var sql = 'SELECT COUNT(*) as count FROM mofointegration.contributorwrapup_totals WHERE ?? = \'1\' && ?? = \'1\';';
  var values = [type1, type2];
  getCountValue(sql, values,
    function (err, res) {
      console.log('>> ', type1, '+', type2, ':', res);
      callback();
    }
  );
}

function getCountForType (type, callback) {
  var sql = 'SELECT COUNT(*) as count FROM mofointegration.contributorwrapup_totals WHERE ?? = \'1\';';
  getCountValue(sql, type,
    function (err, res) {
      console.log(' ');
      console.log(type, ':', res);

      async.eachSeries(contributionTypes,
        function (subType, callback) {
          var currentType = type;
          getCountForTwoTypes(type, subType, function() {
            callback();
          });
        },
        function(err){
        callback();
      });
    }
  );
}


function countByType (callback) {
  async.eachSeries(contributionTypes, getCountForType, function(err){
    callback();
  });
}


function count () {
  console.log(' ');
  console.log(' ');
  console.log('## Start Counting Things ##');
  console.log(' ');

  async.series({

    total_count: function(callback){
      getCountValue('SELECT COUNT(*) as count FROM mofointegration.contributorwrapup_totals;', null,
        function (err, res) {
          console.log('Contributed:', res);
          callback();
        }
      );
    },

    multiple_contribution_types: function(callback){
      getCountValue('SELECT COUNT(*) as count FROM mofointegration.contributorwrapup_totals  WHERE total_contribution_types > 1;', null,
        function (err, res) {
          console.log('Contributed Multiple Ways:', res);
          callback();
        }
      );
    },

    contributors_by_type: function(callback){
      countByType(function () {
        callback();
      });
    }
  },

  function(err, results) {
    // Finished
    console.log(' ');
    console.log( '## Finish Counting Things ##' );
    console.log(' ');
    console.log(' ');
    process.exit(0);
  });
}


count();
