var es = require('elasticsearch');
var _ = require('lodash');
var async = require('async');
var moment = require('moment');
var u = require('util');

client = es.Client({
  host: 'localhost:9200',
  log: 'trace',
  apiVersion: '1.3'
});

var index = "test_data_index";
var type = "test_data_type";

var esWriter = async.cargo(function esBulkLoad(tasks, cb) {

  console.log("t", _.isArray(tasks[1]));

  console.log(tasks);

  if (tasks && tasks.length) {
    return client.bulk({index:index, type:type, body:tasks}, cb);
  }
  return cb();
});

var valMappingTemplate = {
  path_match: '*.*',
  mapping: {
    index: 'not_analyzed',
    type: 'string',
    fields: {
      num: {
        ignore_malformed: true,
        type: 'double'
      },
      date: {
        ignore_malformed: true,
        format: [
          'dateOptionalTime',
          'date_optional_time',
          'yyyy-MM-dd HH: mm:ss.SSS',
          'd-MMM-yy',
          'M/d/yy',
          'yyyy-MM-dd HH: mm:ss',
          'MMM-yy',
          'MMMM yyyy'
        ].join('||'),
        type: 'date'
      },
      searchable: {
        index: 'analyzed',
        type: 'string'
      }
    }
  }
};


var mapping = {mappings:{}};

mapping.mappings[type] = {
  "dynamic_templates": [
    {
      "val_template": valMappingTemplate
    },
    {
      "property_template": {
        "match": "*",
        "mapping": {
          "type": "nested",
          "properties": {
            "start":{ "type":"date" },
            "end":{ "type":"date" }
          },
          "fields": {
            "org": {"type": "{dynamic_type}", "index": "not_analyzed"}
          }
        }
      }
    }
  ]
};

var num_objects = 100;
var num_fields = 300;
var num_days = 365;

var stringSize = 10;
var maxInt = 1000000000;
var maxFloat = 100;
var maxDate = moment('01-01-2016')+0;

var types = [
    function string() {
      return new Array(stringSize).join(String.fromCharCode(Math.floor(Math.random() * 95 + 32)));
    },
    function int() {
      return Math.round(Math.random() * maxInt);
    },
    function float() {
      return Math.random() * maxFloat;
    },
    function date() {
      return moment(maxDate * Math.random()).toISOString();
    }
];


function generate() {

  async.each(_.range(num_objects), function(i, cb) {
    console.log('creating object', i);
    var obj =  _.transform(new Array(num_fields), function (acc, v, i) {
      var now = moment('01-01-2014');
      acc["field" + i] = _.map(new Array(num_days), function () {
        return {
          start: now.toISOString(),
          end: now.add(1, 'day').toISOString(),
          value: types[i % types.length]()
        };
      });
    }, {});

    console.log("t", _.isArray(obj));

    console.log("pushing to cargo", obj.length);
    esWriter.push([{index:{}}, obj]);
    cb();
  });
}

client.indices.delete({index: index}, function() {
  client.indices.create({
    index: index,
    body: mapping,
    ignore: [400] // Ignore "IndexAlreadyExistsException".
  }, generate);
});


