// Query Couch
//
// Arbitrary CouchDB queries.

define(['./jquery.request'], function(req) {

function Query (type) {
  var self = this;

  self.db = "";
  self.ddoc = '_design/example';
  self.include_docs = false;
  self.filters = [];

  self.type = type;
  if(!self.type)
    throw new Error("Document type required");

  function default_callback(er, resp, body) {
    // Default callback for receiving a view response.
    if(window.console && console && console.log)
      console.log('Query response ' + self.url() + ': er:%o resp:%o body:%o', er, resp, body);
  }
  self.callback = default_callback;
}

Query.prototype.filter = function(condition, val) {
  var self = this;

  var match = /^(.*) (.*?)$/.exec(condition);
  if(!match)
    throw new Error('Unknown condition: "'+condition+'"');

  var field = match[1]
    , op    = match[2]
    , filter = { 'field': field
               , 'op'   : op
               , 'value': val
               , 'repr' : function() { return [this.field, this.op, JSON.stringify(this.val)].join(' ') }
               };

  self.filters.push(filter);

  return self.valid();
}

Query.prototype.valid = function() {
  var self = this;

  // Building a URL will confirm validity.
  var url = self.url();
  if(typeof url !== 'string')
    throw new Error('Bad url() return, but no exception: ' + JSON.stringify(url));

  return self;
}

Query.prototype.cb = function(callback) {
  var self = this;
  self.callback = callback;
  return self;
}

Query.prototype.go = function(callback) {
  var self = this;

  if(callback)
    self.cb(callback);

  console.log('TODO: Go!');

  return self;
}

Query.prototype.view = function() {
  var self = this;

  var columns = [ self.type ];
  for(var a = 0; a < self.filters.length; a++)
    columns.push(self.filters[a].field);

  return [ self.db
         , '/'
         , self.ddoc
         , '/_view/QC-'
         , columns.join('-')
         ].join("");
}

Query.prototype.url = function() {
  var self = this;

  var kvs = []
    , do_json = { startkey:1, endkey:1, key:1 }
    , query = self.query()
    ;

  for (var k in query)
    kvs.push(k + '=' + (do_json[k] ? JSON.stringify(query[k]) : query[k]));

  return self.view() + '?' + kvs.join('&');
}

Query.prototype.query = function() {
  var self = this;
  var query = { reduce: false
              , include_docs: self.include_docs
              };

  query.key = [];
  var direction = 'equal';

  self.filters.forEach(function(filter) {
    var op = filter.op;
    debugger;

    if(op === '==' || op === '=') {
      if(direction === 'equal')
        query.key.push(filter.value)
      else
        throw new Error('Filter \''+filter.repr()+'\' not allowed after previous ' + direction + ' filter');
    }

    else if(op === '<=') {
      if(direction !== 'equal' && direction !== 'less')
        throw new Error("Filter '"+filter.repr()+"' not allowed after previous " + direction + "filter");
      else {
        direction = 'less';
        if(query.key) {
          query.startkey = query.key;
          query.endkey = JSON.parse(JSON.stringify(query.key));
          delete query.key;
        }

        query.startkey.push(null); // null sorts before all other types.
        query.endkey.push(filter.value);
      }
    }

    else if(op === '>=') {
      if(direction !== 'equal' && direction !== 'greater')
        throw new Error("Filter '"+filter.repr()+"' not allowed after previous " + direction + "filter");
      else {
        direction = 'greater';
        if(query.key) {
          query.startkey = query.key;
          query.endkey = JSON.parse(JSON.stringify(query.key));
          delete query.key;
        }

        query.startkey.push(filter.value);
        query.endkey.push({}); // Object sorts after all other types.
      }
    }

    else
      throw new Error('Unknown filter operation: ' + JSON.stringify(op));
  })

  return query;
}

Query.prototype.doc   = function() { this.include_doc = true ; return this };
Query.prototype.nodoc = function() { this.include_doc = false; return this };

var q = new Query('Page').filter('created_at <=', new Date).doc().cb(function(er, resp, body) {
  if(er) throw er;
  debugger;
  return 1;
})

console.log('url: %o', q.url());
throw new Error('done');

return Query;

});
