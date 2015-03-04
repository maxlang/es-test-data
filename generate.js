var es = require('elasticsearch');
var _ = require('lodash');
var async = require('async');
var moment = require('moment');

client = es.Client({
  host: 'localhost:9200',
  log: 'error',
  apiVersion: '1.3'
});

var index = "test_data_index";
var type = "test_data_type";
var activity_type = "test_activity_data_type";

var esWriter = async.cargo(function esBulkLoad(tasks, cb) {
  console.log("shipping");
  if (tasks && tasks.length) {
    return client.bulk({body:tasks, refresh:false}, cb);
  }
  return cb();
}, 32); //must be power of 2

var valMappingTemplate = {
  match: '*',
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
        type: 'string',
        analyzer: 'trim_lowercase_keyword_analyzer'
      }
    }
  }
};


var mapping = {
  settings: {
    index: {
      number_of_replicas: 0,
          number_of_shards: 4,

        // Here we add an analyzer named "trim_lowercase_keyword_analyzer" that will strip whitespace and trim strings
        // then store them whole (up to 256 characters). We'll use this in place of non-analyzed strings because it
        // makes the app matching logic case insensitive and more consistent over different whitespace
          analysis: {
        analyzer: {
          trim_lowercase_keyword_analyzer: {
            type: 'custom',
                tokenizer: 'keyword',
                filter: [
              'lowercase',
              'trim'
            ],
                char_filter: []
          }
        },
        tokenizer: {
          keyword: {
            type: 'keyword'
          }
        },
        filter: {
          trim: {
            type: 'trim'
          },
          lowercase: {
            type: 'lowercase'
          }
        }
      }
    }
  },mappings:{}};

mapping.mappings[type] ={
  "dynamic_templates": [
    {
      "property_template": {
        "match": "*",
        "match_mapping_type" : "object",
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
    },
    {
      "val_template": valMappingTemplate
    }
  ]
};
mapping.mappings[activity_type] = _.cloneDeep(mapping.mappings[type]);
mapping.mappings[activity_type]._parent = {
  type: type
};

console.log('---');
console.log(JSON.stringify(mapping));
console.log('---');

var num_objects = 5000;
var num_fields = 180;
var num_days = 5;
var activities = 100;

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

types.string = types[0];
types.int = types[1];
types.float = types[2];
types.date = types[3];

var timeout = 0;

function generate() {
  
  client.indices.putSettings({index:index, body: {index:{refresh_interval:-1}}});

  console.log('generate topics');

  async.eachSeries(_.range(num_objects), function(i, cb) {
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
    obj._id = i;
    esWriter.push([{index:{_id:obj._id, _index:index, _type:type}}, obj]);
    console.log("q length", esWriter.length(), esWriter.payload);
    if (esWriter.length() > esWriter.payload) {
      setTimeout(function() {
        console.log('paused');
        cb();
      }, timeout);
    } else {
      timeout = Math.max(0, timeout - 10);
      cb();
    }
    if (esWriter.length() > (esWriter.payload*2)) {
      //esWriter.payload = Math.max(2, esWriter.payload/2);
      timeout += 10;
    }
  }, function (err) {
    if (err) console.error(err);

    console.log('done with topics');
    generateActivity();
  });
}

function generateActivity() {

  client.indices.putSettings({index:index, body: {index:{refresh_interval:-1}}});

  console.log('generate activity');

  async.eachSeries(_.range(num_objects), function(i, cb) {
    console.log('adding activity for object', i);

    async.eachSeries(_.range(activities), function(j, cb) {
      var obj = {
        _id: i + '_' + j,
        //_parent: i,
        user: types.string(),
        time: types.date(),
        verb: types.string(),
        data: types.float()
      };
      esWriter.push([{index:{_id:obj._id, _parent:i, _index:index, _type:activity_type}}, obj]);
      console.log("q length", esWriter.length(), esWriter.payload);
      if (esWriter.length() > esWriter.payload) {
        setTimeout(function() {
          console.log('paused');
          cb();
        }, timeout);
      } else {
        timeout = Math.max(0, timeout - 10);
        cb();
      }
      if (esWriter.length() > (esWriter.payload*2)) {
        //esWriter.payload = Math.max(2, esWriter.payload/2);
        timeout += 10;
      }
    }, cb);
  }, function (err) {
    if (err) console.error(err);

    console.log('done with activities');
    client.indices.refresh({index: index});

    client.indices.putSettings({index:index, body: {index:{refresh_interval:'1s'}}});
  });
}

client.indices.delete({index: index}, function() {
  client.indices.create({
    index: index,
    body: mapping,
    ignore: [400] // Ignore "IndexAlreadyExistsException".
  }, generate);
});


