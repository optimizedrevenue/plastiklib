/* #declare require, module, JSON, Buffer, encodeURIComponent, Error; */      
/* #include trycatch, undef, once; */   

var http = require('http');
var querystring = require('querystring');

var viewQueryKeys = [
	'descending', 'endkey', 'endkey_docid', 'group',
	'group_level', 'include_docs', 'inclusive_end', 'key',
	'limit', 'reduce', 'skip', 'stale',
	'startkey', 'startkey_docid', 'update_seq'
];

var changesQueryKeys = ['filter', 'include_docs', 'limit', 'since', 'timeout'];

module.exports = Couch;

function Couch(port, host) { var couch = this;
	couch.port = port;
	couch.host = host;
}

Couch.prototype = {
	reqOpts: function (method, path, headers) { var couch = this;
		var opts = {
			host: couch.host,
			port: couch.port,
			path: path,
			method: method,
			headers: headers || {}
		};
		
		opts.headers.host = couch.host;
		
		return opts;
	},
	
	processResponse: function (request, next) { var couch = this;
		waitForResponse(request, onWaitForResponse);
		
		function onWaitForResponse(__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode >= 300 || response.headers['content-type'] === 'application/json') {
				readAllText(response, response.statusCode >= 300 ? 8192 : null, onReadAllText);
			}
			else {
				next(null, response);
			}
			
			function onReadAllText(__err, buffer) { if (__err) { next(__err); return; }
				response.body = buffer;
				
				if (response.headers['content-type'] === 'application/json') {
					trycatch(function (_) { return JSON.parse(response.body); }, onParsed);
				}
				else {
					next(null, response);
				}
			}
			
			function onParsed(__err, json) { if (__err) { next(__err); return; }
				response.json = json;
				next(null, response);
			}
		}
	},
	
	GET: function (path, headers, next) { var couch = this;
		headers = headers || {};
		if (!headers.accept) {
			headers.accept = 'application/json';
		}
	
		var request = http.request(couch.reqOpts('GET', path, headers));
		couch.processResponse(request, next);
	},
	
	DELETE: function (path, headers, next) { var couch = this;
		headers = headers || {};
		if (!headers.accept) {
			headers.accept = 'application/json';
		}
	
		var request = http.request(couch.reqOpts('DELETE', path, headers));
		couch.processResponse(request, next);
	},
	
	_put_or_post: function (which, path, body, headers, next) { var couch = this;
		headers = headers || {};
		if (!headers.accept) {
			headers.accept = 'application/json';
		}
		
		if (typeof(body) === 'object' && !headers['content-type']) {
			body = JSON.stringify(body);
			headers['content-type'] = 'application/json';
		}
		else if (typeof(body) === 'string') {
			body = new Buffer(body, 'utf8');
		}
		
		var request = http.request(couch.reqOpts(which, path, headers));
		request.write(body);
		couch.processResponse(request, next);
	},
	
	PUT: function (path, body, headers, next) { var couch = this;
		couch._put_or_post('PUT', path, body, headers, next);
	},
	
	POST: function (path, body, headers, next) { var couch = this;
		couch._put_or_post('POST', path, body, headers, next);
	},
	
	db: function (name) { var couch = this;
		return new DbHandle(couch, name);
	}
};

function DbHandle(couch, name) { var db = this;
	db.couch = couch;
	db.name = name;
}

DbHandle.prototype = {
	docUrl: function (docId) { var db = this;
		if (docId.indexOf('_design/') !== 0) {
			docId = encodeURIComponent(docId);
		}
		return '/' + db.name + '/' + docId;
	},
	
	info: function (next) { var db = this;
		db.couch.GET('/' + db.name, null, function (__err, response) { if (__err) { next(__err); return; }
			next(null, response.json);
		});
	},

	getDoc: function (docId, next) { var db = this;
		db.couch.GET(db.docUrl(docId), null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode === 404) { next(null, null); return; }
			if (response.statusCode !== 200) { next(new Error('error getting doc ' + docId + ': ' + response.body)); return; }
			next(null, response.json);
		});
	},
	
	getDocWhere: function (docId, condition, next) { var db = this;
		db.getDoc(docId, function (__err, doc) { if (__err) { next(__err); return; }
			if (doc !== null && condition(doc)) {
				next(null, doc);
			}
			else {
				next(null);
			}
		});
	},
	
	putDoc: function (doc, opts, next) { var db = this;
		if (undef(next)) {
			next = opts;
			opts = null;
		}
		
		var url = db.docUrl(doc._id);
		if (opts && opts.batch) {
			url += "?batch=ok";
		}
		
		db.couch.PUT(url, doc, null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode === 201 || response.statusCode === 202 || (response.statusCode === 409 && opts && opts.conflictOk)) {
				next(null, response.json);
			}
			else {
				next(new Error('error putting doc ' + doc._id + ': ' + response.body));
			}
		});
	},
	
	updateDoc: function (docId, fn, next) { var db = this;
		tryIt();
	
		function tryIt() {
			db.getDoc(docId, onGot);
		}
		
		function onGot(__err, doc) { if (__err) { next(__err); return; }
			if (doc === null) {
				doc = {_id: docId};
			}
			fn(doc, onApplied);
		}
		
		function onApplied(__err, doc) { if (__err) { next(__err); return; }
			db.putDoc(doc, {conflictOk: true}, onPut);
		}
		
		function onPut(__err, response) { if (__err) { next(__err); return; }
			if (response.ok) {
				next(null, response);
			}
			else {
				tryIt();
			}
		}
	},
	
	deleteDoc: function (docId, rev, opts, next) { var db = this;
		if (undef(next)) {
			next = opts;
			opts = null;
		}
	
		var url = db.docUrl(docId) + '?rev=' + encodeURIComponent(rev);
		
		db.couch.DELETE(url, null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode === 200 || (response.statusCode === 409 && opts && opts.conflictOk)) {
				next(null, response.json);
			}
			else {
				next(new Error('error deleting doc ' + docId + ': ' + response.body));
			}
		});
	},
	
	viewQuery: function (path, query, next) { var db = this;
		if (undef(next)) {
			next = query;
			query = null;
		}
	
		query = query || {};
		var url = '/' + db.name + '/' + path;
		var q = {};
		viewQueryKeys.forEach(function (key) {
			if (!undef(query[key])) {
				q[key] = JSON.stringify(query[key]);
			}
		});
		
		db.couch.GET(url + '?' + querystring.stringify(q), null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode !== 200) {
				next(new Error('error reading view ' + path + ': ' + response.body));
			}
			else {
				next(null, response.json);
			}
		});
	},
	
	view: function (designName, viewName, query, next) { var db = this;
		db.viewQuery('_design/' + designName + '/_view/' + viewName, query, next);
	},
	
	allDocs: function (query, next) { var db = this;
		db.viewQuery('_all_docs', query, next);
	},
	
	viewKeysQuery: function (path, keys, next) { var db = this;
		var url = '/' + db.name + '/' + path;
		db.couch.POST(url, {keys: keys}, null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode !== 200) {
				next(new Error('error reading view ' + path + ': ' + response.body));
			}
			else {
				next(null, response.json);
			}
		});
	},
	
	viewKeys: function (designName, viewName, keys, next) { var db = this;
		db.viewKeysQuery('_design/' + designName + '/_view/' + viewName, keys, next);
	},
	
	allDocsKeys: function (keys, next) { var db = this;
		db.viewKeysQuery('_all_docs', keys, next);
	},
	
	postBulkDocs: function (docs, allOrNothing, next) { var db = this;
		if (undef(next)) {
			next = allOrNothing;
			allOrNothing = false;
		}
		
		var url = '/' + db.name + '/_bulk_docs';
		db.couch.POST(url, {docs: docs, all_or_nothing: allOrNothing}, null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode !== 201) {
				next(new Error('error posting to _bulk_docs:' + response.body));
			}
			else {
				next(null, response.json);
			}
		});
	},
	
	changes: function (query, next) { var db = this;
		if (undef(next)) {
			next = query;
			query = null;
		}
	
		query = query || {};
		var q = {};
		changesQueryKeys.forEach(function (key) {
			if (!undef(query[key])) {
				q[key] = JSON.stringify(query[key]);
			}
		});
		
		if (query.longpoll === true) {
			q.feed = 'longpoll';
		}

		db.couch.GET('/' + db.name + '/_changes?' + querystring.stringify(q), null, function (__err, response) { if (__err) { next(__err); return; }
			if (response.statusCode !== 200) {
				next(new Error('error reading _changes: ' + response.body));
			}
			else {
				next(null, response.json);
			}
		});
	}
	
	/*
	updateAll: function(fn, next) {
		Frame(function(resume, wait) {
			function kick(err, value) {
				if (err) { next(err); return; }
				resume(value);
			}
		
			this.info(kick);
			var info = wait();
			var maxSeq = info.update_seq;
			var lastSeq = 0;
			var docsChanged = 0;
			var changes, change, i, doc, shouldChange, putResponse;
			while (lastSeq < maxSeq) {
				// get the next 1000 changes
				this.changes({since: lastSeq, include_docs: true, limit: 1000}, kick);
				changes = wait();
				
				// inspect each of those docs
				for (i = 0; i < changes.length; i++) {
					change = changes[i];
					if (change.seq <= maxSeq) {
						lastSeq = change.seq;
						doc = change.doc;
						while (true) {
							fn(doc, kick);
							shouldChange = wait();
							if (shouldChange) {
								this.putDoc(doc, {conflictOk: true}, kick);
								putResponse = wait();
								if (putResponse.ok) {
									docsChanged += 1;
									break;
								}
								else {
									this.getDoc(doc._id, kick);
									doc = wait();
								}
							}
							else {
								break;
							}
						}
					}
				}
			}
			next(null, {ok: true, docsChanged: docsChanged, maxSeq: maxSeq});
		});
	},
	*/
};

function waitForResponse(request, next) {
	next = once(next);
	
	request.on('error', next);
	
	request.on('response', function (response) {
		next(null, response);
	});
	
	request.end();
}

function readAllText(stream, limit, next) {
	next = once(next);
	var buffer = '';
	stream.encoding = 'utf8';
	
	stream.on('data', function (chunk) {
		if (!limit || buffer.length < limit) {
			buffer += chunk;
		}
	});
	
	stream.on('error', next);
	
	stream.on('end', function () {
		next(null, buffer);
	});
}

function trycatch(fn, next) {
	try {
		next(null, fn());
	}
	catch (err) {
		next(err);
	}
}

function undef(x) {
	return typeof x === 'undefined';
}

function once(f) {
	var called = false;
	return function() {
		if (!called) {
			called = true;
			return f.apply(this, arguments);
		}
	};
}
