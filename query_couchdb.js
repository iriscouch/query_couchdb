// Query Couch
//
// Arbitrary CouchDB queries.

define(['./jquery.request'], function(req) {

function Query (type) {
  var self = this;

  self.ddoc = Query.ddoc || '_design/example';
  self.include_docs = false;
  self.query_limit = null;
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

Query.ddoc = null;

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

Query.prototype.order = function(instruction) {
  var self = this
    , match
    ;
  if(match = /^(-?)(.+)$/.exec(instruction)) { // Assignment expression
    var dir = match[1]
      , field = match[2]
      ;

    dir = { '-': 'down'
          , '+': 'up'
          , '' : 'up'
          }[dir];

    if(!dir)
      throw new Error('Unknown order instruction: ' + JSON.stringify(instruction));

    var filter = { 'field': field
                 , 'order': dir
                 , 'repr' : function() { return 'order ' + dir + ' ' + field }
                 };

    self.filters.push(filter);

    return self.valid();
  }
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

  req.json({uri:self.url()}, function(er, resp, body) {
    if(er) throw er;

    if(resp.status === 200)
      return self.callback && self.callback(null, body);

    else if(resp.status !== 404 || body.error !== 'not_found')
      return self.callback && self.callback(new Error('Unknown response during query ' + self.url() + ': ' + JSON.stringify(body)));

    else if(resp.status === 404 && body.error === 'not_found') {
      // Need to create this query.
      var fields = [];
      self.filters.forEach(function(filter) {
        fields.push('doc[' + JSON.stringify(filter.field) + '] || null');
      })
      fields = '[' + fields.join(', ') + ']';

      var map = [ 'function(doc) {'
                , '  var re = /^' + self.type + '\\//;'
                , '  var match = re.exec(doc._id);'
                , '  if(match) {'
                , '    emit('+fields+', doc);'
                , '  }'
                , '}'
                ].join('\n');

      var reduce = '_count';

      function store_view(tries) {
        if(tries <= 0)
          throw new Error("No more tries remain to store the view: " + self.url());

        req.couch({uri:self.ddoc}, function(er, resp, ddoc) {
          if(er) return self.callback && self.callback(er);

          ddoc.views = ddoc.views || {};
          ddoc.views[self.name()] = { 'map':map, 'reduce':reduce };
          req.json({method:'PUT', uri:self.ddoc, body:JSON.stringify(ddoc)}, function(er, resp, body) {
            if(er) return self.callback && self.callback(er);

            if(resp.status === 409) {
              console.debug('Retrying on conflict...');
              return store_view(tries - 1);
            }

            else if(resp.status !== 201)
              return self.callback && self.callback(new Error('Error when storing view: ' + JSON.stringify(body)));

            // View saved. One more try.
            req.json({uri:self.url()}, function(er, resp, body) {
              if(er) throw er;

              if(resp.status === 200)
                return self.callback && self.callback(null, body);
              else
                return self.callback && self.callback(new Error('Failed to after creating ' + self.url() + ': ' + JSON.stringify(body)));
            })
          })
        })
      } // store_view

      return store_view(5);
    }
  })

  return self;
}

Query.prototype.view = function() {
  var self = this;


  return [ self.ddoc
         , '/_view/'
         , self.name()
         ].join('');
}

Query.prototype.name = function() {
  var self = this;

  var columns = [ 'QC', self.type ];
  for(var a = 0; a < self.filters.length; a++)
    columns.push(self.filters[a].field);

  return columns.join('-');
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
              , descending: self.descending
              , include_docs: self.include_docs
              };

  if(typeof self.query_limit === 'number')
    query.limit = self.query_limit;

  query.key = [];
  var direction = 'equal';

  self.filters.forEach(function(filter) {
    var op = filter.op;

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

    else if(!op && filter.order) {
      if(filter.order === 'down' && direction === 'equal')
        query.descending = true;
      else if(filter.order === 'up' && direction === 'equal')
        query.descending = false;
      else
        throw new Error('Cannot filter: ' + JSON.stringify(filter));
    }

    else
      throw new Error('Unknown filter operation: ' + JSON.stringify(op));
  })

  if(query.key && query.key.length === 0)
    delete query.key;

  return query;
}

Query.prototype.doc   = function() { this.include_doc = true ; return this };
Query.prototype.nodoc = function() { this.include_doc = false; return this };
Query.prototype.limit = function(lim) { this.query_limit = lim; return this };

return Query;

});
