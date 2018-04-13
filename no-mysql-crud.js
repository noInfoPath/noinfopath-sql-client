/*
 *	[NoInfoPath Home](http://gitlab.imginconline.com/noinfopath/noinfopath/wikis/home) | [NoInfoPath REST API](home)
 *
 *	___
 *
 *	*@version 2.0.20*
 *
 *	Copyright (c) 2017 The NoInfoPath Group, LLC.
 *
 *	Licensed under the MIT License. (MIT)
 *
 *	___
 *
 *	MongoDB CRUD Provider Configuration
 *	-----------------------------
 *
 *	A CRUD provider's configuration consist of required and optional configuration properties.
 *	And, each may have properties specific to themselves. This section explains
 *	how and when to use them.
 *
 *	### MongoDB Properties
 *
 *	|Name|Description|
 *	|----|-----------|
 *	|storageType|Always `mongo`|
 *	|mongoDbUrl|A url that points to the mongodb server, and the database to connect to.|
 *	|uri|The URI that use to configure the route for the end-point.|
 *	|collectionName|The name of the collection within the database specified by the `mongoDbUrl`|
 *	|primaryKey|The property (column) that defined the collections primary key.|
 *
 *	**Sample Configuration**
 *
 *	```json
 *	{
 *		"storageType": "mongo",
 *		"mongoDbUrl": "mongodb://localhost:27017/efr2_dtc",
 *		"uri": "dtc/changes-metadata",
 *		"collectionName": "changes.files",
 *		"primaryKey": "_id"
 *	}
 *	```
 */
var config = {
		mysql: {}
	},
	util = require("util"),
	mysql = require("mysql2"),
	//mysqlp = require('promise-mysql'), //TODO: Convert to use promised mysql.
	CRUD = {},
	CRUD_OPERATIONS = {
		"CREATE": "create",
		"READ": "read",
		"EXECSP": "execSP",
		"READMETA": "readMeta",
		"UPDATE": "update",
		//"WRITESP": "writeSP",
		"DELETE": "delete",
		"COUNT": "count"
	};

function _resolveData(indata) {
	var d = indata;
	if (typeof (d) === "string") {
		d = JSON.parse(d);
	}
	return d;
}

function _countDocuments(collection, data, filter) {
	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql),
			sql = "select count(*) as total from ??",
			qstuff = [collection];

		if (filter.query) {
			sql = sql + " WHERE " + filter.query.where;
			qstuff = qstuff.concat(filter.query.parameters);
		}

		connection.connect();

		connection.query(sql, qstuff, function (error, results, fields) {
			connection.end();
			if (error) {
				reject(error);
			} else {
				resolve(results[0] ? results[0].total : 0);
			}
		});

	});

}
CRUD[CRUD_OPERATIONS.COUNT] = _countDocuments;

function _readDocument(collection, data, filter) {
	return new Promise(function (resolve, reject) {

		var connection = mysql.createConnection(config.mysql),
			sql = "select ?? from ??",
			qstuff = [filter.select || "*", collection];

		//console.log("_readDocument", connection.state);
		if(filter) {
			if (filter.query) {
				//console.log(filter.query.parameters);
				sql = sql + " WHERE " + filter.query.where.replace(/\$\d+/gi, "?");
				qstuff = qstuff.concat(filter.query.parameters);
			} else {

			}
		}


		if (filter.orderby) {
			sql = sql + " ORDER BY " + filter.orderby;
		}

		if (filter.limit) {
			sql = sql + " LIMIT " + filter.limit;
		}

		connection.connect();

		//console.log(sql, qstuff);

		connection.query(sql, qstuff, function (error, results, fields) {
			connection.end();

			if (error) {
				reject(error);
			} else {
				if (filter.getTotal) {
					var retval = {
						value: results
					};
					return _countDocuments(collection, data, filter)
						.then(function (total) {
							retval["odata.metadata"] = true;
							retval["odata.count"] = total;
							resolve(retval);
						});
				} else {
					resolve(results);
				}
			}
		});
	});
}
CRUD[CRUD_OPERATIONS.READ] = _readDocument;

function _readStoredProcedure(collection, data, tmp, multi) {
	//console.log(data, tmp);
	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql),
			sqlFormat = "call %s('%s')",
			sql,
			qstuff = [], params = {};

		// if (tmp.paramNames) {
		// 	// sql = sql + "("; // open
		// 	// this was a forEach but I needed a good way to not have the trailing comma
		// 	var paramString = tmp.paramNames.map(function(param){
		// 		return "?";
		// 	}).join(",");
		//
		// 	var paramValues = tmp.paramNames.map(function(param){
		// 		return tmp.query[param];
		// 	});
		//
		// 	qstuff = qstuff.concat(paramValues);
		// 	sql = util.format(sqlFormat, collection, paramString);
		//
		// } else {
		if(tmp && tmp.query) {
			for(var i=0; i < tmp.query.parameters.length; i+=2) {
				var n = tmp.query.parameters[i],
					v = tmp.query.parameters[i+1];

				params[n] = v && typeof(v) === "string" ? v.replace(/'/gi, "") : v;
			}
		}

		if(data) {
			params = Object.assign(params, data);
		}

		sql = util.format(sqlFormat, collection, JSON.stringify(params));

		//}

		connection.connect();

		connection.query(sql, qstuff, function (error, results, fields) {
			connection.end();
			console.log(results);
			if (error) {
				reject(error);
			} else {
				resolve(results.length > 0 ? (multi ? results : results[0]) : []);
			}
		});
	});
}
CRUD[CRUD_OPERATIONS.EXECSP] = _readStoredProcedure;

function _insertDocument(collection, data, filter) {

	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql);

		connection.connect(function (err, status) {
			if (err) {
				console.error(err);
				reject(err);
			} else {
				connection.query("INSERT INTO ??  SET ?", [collection, data], function (error, results, fields) {
					connection.end(function (err, response) {
						if (error) {
							reject(error);
						} else {
							resolve(results);
						}
					});

				});
			}
		});
	});
}
CRUD[CRUD_OPERATIONS.CREATE] = _insertDocument;

function _writeStoredProcedure(collection, data, filter) {
	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql),
			sqlFormat = "call %s(%s)",
			sql,
			qstuff = [];

		if(data) {
			sql = util.format(sqlFormat, collection, "?");
			qstuff.push(JSON.stringify(data));
		} else {
			sql = util.format(sqlFormat, collection, "");
		}

		connection.connect();

		connection.query(sql, qstuff, function (error, results, fields) {
			connection.end();

			if (error) {
				reject(error);
			} else {
				resolve(results);
			}
		});
	});
}
CRUD[CRUD_OPERATIONS.WRITESP] = _writeStoredProcedure;

function _updateDocument(collection, data, filter) {
	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql),
			key = Object.keys(filter)[0],
			val = filter[key],
			qstuff = [collection, data, key, val];

		connection.connect(function (err, r2) {

			if (err) {
				resolve(err);
			} else {
				connection.query("UPDATE ?? SET ? WHERE ?? = ?", qstuff, function (error, results, fields) {

					connection.end();
					if (error) {
						reject(error);
					} else {

						resolve(results);
					}

				});
			}

		});

	});

}
CRUD[CRUD_OPERATIONS.UPDATE] = _updateDocument;

function _deleteDocument(collection, data, filter) {
	return new Promise(function (resolve, reject) {
		var connection = mysql.createConnection(config.mysql);

		connection.connect();

		if (Array.isArray(filter)) {
			connection.query("DELETE FROM ?? WHERE ?? = ?", filter, function (error, results, fields) {
				connection.end();

				if (error) {
					reject(error);
				} else {
					resolve(results);
				}
			});
		} else {
			connection.query("DELETE FROM ?? WHERE " + filter.query.where, [collection].concat(filter.query.parameters), function (error, results, fields) {
				connection.end();
				if (error) {
					reject(error);
				} else {
					resolve(results);
				}
			});
		}



	});
}
CRUD[CRUD_OPERATIONS.DELETE] = _deleteDocument;

/*
*	_resolveFilter
*/
function _resolveFilter(req) {
	function __isReqObject(req) { return !!req.url; }
	function __hasQueryString(req) { return __isReqObject(req) && (Object.keys(req.query || {}).length > 0); }
	function __hasRouteParams(req) { return __isReqObject(req) && (Object.keys(req.params || {}).length > 0); }
	function __hasODATA(req) { return __isReqObject(req) && (Object.keys(req.odata || {}).length > 0); }

	if(!req) {
		return {type: "none", filter: null};
	} else if(__hasODATA(req)) {
		return {type: "odata", filter: req.odata};
	} else if(__hasQueryString(req)){
		return {type: "queryString", filter: req.query};
	} else if(__hasRouteParams(req)) {
		return {type: "routeParams", filter: req.params};
	} else {
		if(__isReqObject(req)){
			return req;
		} else {
			if(typeof(req) === "object") {
				return {type: "plainObject", filter: req};
			} else {
				return {type: "routeParams", filter: {"id": req}};
			}
		}

	}

}

function _makeQueryPayload(filter) {
	var q = {
			"select": "*",
			"where": "",
			"orderby": "",
			"parameters": []
		}, tWhere = [];

	Object.keys(filter).forEach(function(k){
		tWhere.push("?? = ?");
		q.parameters.push(k);
		q.parameters.push(filter[k]);

	});

	q.where = tWhere.join(" and ");

	return {query: q};
}

function beginTransaction(schema, type, data, filter, multi) {
	var resolvedFilter = _resolveFilter(filter),
		tmp = {select: "*"}, entityName;

	switch(type) {
		case CRUD_OPERATIONS.DELETE:
			if (filter.params && filter.params.id) {
				tmp = [schema.entityName, schema.primaryKey, filter.params.id];
			} else {
				tmp = filter.odata;
			}
			break;


		case CRUD_OPERATIONS.UPDATE:
			if(resolvedFilter.type !== "plainObject") {
				resolvedFilter.filter = {};
				resolvedFilter.filter[schema.primaryKey] = data[schema.primaryKey];
			}
			tmp = resolvedFilter.filter;

			break;
		default:
			switch(resolvedFilter.type) {
				case "odata":
					tmp = Object.assign(resolvedFilter.filter);
					break;
				case "routeParams":
				case "queryString":
				case "plainObject":
					tmp = _makeQueryPayload(resolvedFilter.filter);
					break;
				default:
					break;
			}

	};

	if(schema.activeUri && schema.activeUri.type === "sp") {
		entityName = schema.activeUri.name;
	} else {
		entityName = schema.entityName;
	}

	return CRUD[type](entityName, data, tmp, multi)
		.then(function (results) {
			return results;
		})
		.catch(function (err) {
			console.error(err);
			throw err;
		});
}

module.exports = function (sqlConnInfo) {
	config.mysql = sqlConnInfo;

	return {
		type: "CRUD",
		crudType: "mysql",
		execute: beginTransaction,
		operations: CRUD_OPERATIONS
	};
};
