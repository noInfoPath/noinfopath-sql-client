var util = require("util"),
	crudInterfaces = {
		"mysql": __dirname + "/no-mysql-crud"
	},
	conversionFunctionsIn = {
		"bigint": function (i) {
			return i;
		},
		"bit": function (i) {
			return i;
		},
		"decimal": function (n) {
			return n;
		},
		"int": function (i) {
			return i;
		},
		"money": function (n) {
			return n;
		},
		"numeric": function (n) {
			return n;
		},
		"smallint": function (i) {
			return i;
		},
		"smallmoney": function (n) {
			return n;
		},
		"tinyint": function (i) {
			return i;
		},
		"float": function (i) {
			return i;
		},
		"real": function (i) {
			return i;
		},
		"date": function (n) {
			return n ? n : null;
		},
		"datetime": function (n) {
			return n ? n.replace(/T/gi, " ").replace(/Z/gi, "") : null;
		},
		"datetime2": function (n) {
			return n ? n.replace(/T/gi, " ").replace(/Z/gi, "") : null;
		},
		"datetimeoffset": function (n) {
			return n ? n : null;
		},
		"smalldatetime": function (n) {
			return n ? n : null;
		},
		"time": function (n) {
			return n ? n : null;
		},
		"char": function (t) {
			return t;
		},
		"nchar": function (t) {
			return t;
		},
		"varchar": function (t) {
			return t;
		},
		"nvarchar": function (t) {
			return t;
		},
		"text": function (t) {
			return t;
		},
		"ntext": function (t) {
			return t;
		},
		"binary": function (i) {
			return i;
		},
		"varbinary": function (i) {
			return i;
		},
		"image": function (i) {
			return i;
		},
		"uniqueidentifier": function (t) {
			return t;
		}
	},
	conversionFunctionsOut = {
		"bigint": function (i) {
			return i;
		},
		"bit": function (i) {
			return i;
		},
		"decimal": function (n) {
			return n;
		},
		"int": function (i) {
			return i;
		},
		"money": function (n) {
			return n;
		},
		"numeric": function (n) {
			return n;
		},
		"smallint": function (i) {
			return i;
		},
		"smallmoney": function (n) {
			return n;
		},
		"tinyint": function (i) {
			return i;
		},
		"float": function (i) {
			return i;
		},
		"real": function (i) {
			return i;
		},
		"date": function (n) {
			return n ? n : null;
		},
		"datetime": function (n) {
			if(typeof(n) === "string") {
				n =  n.replace(/ /gi, "T");
				n =  n.indexOf("Z") === -1 ? n + "Z" : n;
			}

			return n ? n  : null;
		},
		"datetime2": function (n) {
			if(typeof(n) === "string") {
				n =  n.replace(/ /gi, "T");
				n =  n.indexOf("Z") === -1 ? n + "Z" : n;
			}

			return n ? n  : null;
		},
		"datetimeoffset": function (n) {
			return n ? n : null;
		},
		"smalldatetime": function (n) {
			return n ? n : null;
		},
		"time": function (n) {
			return n ? n : null;
		},
		"char": function (t) {
			return t;
		},
		"nchar": function (t) {
			return t;
		},
		"varchar": function (t) {
			return t;
		},
		"nvarchar": function (t) {
			return t;
		},
		"text": function (t) {
			return t;
		},
		"ntext": function (t) {
			return t;
		},
		"binary": function (i) {
			return i;
		},
		"varbinary": function (i) {
			return i;
		},
		"image": function (i) {
			return i;
		},
		"uniqueidentifier": function (t) {
			return t;
		}
	};

function isNumber(i) {
	return !Number.isNaN(Number(i)) && i !== null;
}

function _nullable(pk, col, prop) {
	if ((prop !== null) || (col.nullable && prop === null) || (prop === null && col.columnName === pk))
		return prop;

	throw new Error(util.format("Column %s of type %s is not nullable.", col.columnName, col.type));
}

function _scrubDatum(schema, datum) {
	for (var p in datum) {

		try {
			var prop = datum[p],
				col = schema.columns[p],
				cfn;

			if(!col) throw new Error(util.format("%s is not a column in the data table.  Check  your spelling and try again.", p));

			cfn = conversionFunctionsIn[col.type];

			if(cfn) {
				prop = _nullable(schema.primaryKey, col, cfn(prop));
			} else {
				prop = _nullable(schema.primaryKey, col, prop);
			}

			datum[p] = prop;

		} catch (err) {
			console.error(err);
		}
	}

	return datum;
}

function _transformDatum(schema, datum) {
	if(!schema.columns) return datum;

	for (var p in datum) {
		try {
			var prop = datum[p],
				col = schema.columns[p],
				cfn;

			if(!col) throw new Error(util.format("%s is not a column in the data table.  Check  your spelling and try again.", p));

			cfn = conversionFunctionsOut[col.type];

			if(cfn) {
				prop = _nullable(schema.primaryKey, col, cfn(prop));
			} else {
				prop = _nullable(schema.primaryKey, col, prop);
			}

			datum[p] = prop;

		} catch (err) {
			console.error(err);
		}
	}
	return datum;
}

function _resolveBodyData(req) {
	try {
		var b = typeof (req.body) === "string" ? JSON.parse(req.body) : req.body;

		if (b._id) {
			//console.log("HERE");
			delete b._id;
			//console.log("b", b);
		}

		req.body = b;
		return req;

	} catch (err) {
		console.error(err);
		return req;
	}
}

//HTTP Aliases
function _get(crud, schema, req) {
	return _read(crud, schema, req.odata);
}

function _getOne(crud, schema, req) {
	return _one(crud, schema,  req.params.id); //TODO: make this support ODATA filter also.
}

function _putByPrimaryKey(crud, schema, req) {
	//console.log("_putByPrimaryKey", crud.type, "isBucketStorage", _isBucketStorage(schema.storageType));
	var filter = {},
		data = _scrubDatum(schema, _resolveBodyData(req).body),
		routeID = (req.params && req.params.id) || data[schema.primaryKey];

	filter[schema.primaryKey] = routeID;

	return _update(crud, schema, data, filter);

}

function _post(crud, schema, req) {
	var data = req.body;
	return _create(crud, schema, data)
		.then(function(result){
			return req.mydata ? result : data;  //??? what does mydata do?
		});
}

function _delete(crud, schema, req) {
	return _destroy(crud, schema, req);  //TODO: need to resolve filter from req here.
}

//CRUD Aliases
function _create(crud, schema, data) {
	if (data._id) delete data._id;  //This is a mongodb thing.

	if (data[schema.primaryKey]) delete data[schema.primaryKey];

	data = _scrubDatum(schema, data);

	return crud.execute(schema, crud.operations.CREATE, data)
		.then(function(result){
			data[schema.primaryKey] = result.insertId;
			var  r = _transformDatum(schema, data);
			return  r;
		});
}

function _read(crud, schema, filter) {
	return crud.execute(schema, crud.operations.READ, null, filter)
		.then(function(results){

			if(results.value) {
				results.value = results.value.map(function(datum){
					return _transformDatum(schema, datum);
				});
			} else {
				if(Array.isArray(results)) {
					results = results.map(function(datum){
						return _transformDatum(schema, datum);
					});
				} else {
					return results;
				}
			}

			return results;
		});
}

function _one(crud, schema, filter) {
	return _read(crud, schema, filter)
		.then(function (results) {
			return results.length > 0 ? _transformDatum(schema, results[0]) : null;
		});
}

function _update(crud, schema, data, filter) {
	var pk = data[schema.primaryKey];

	delete data[schema.primaryKey];

	return crud.execute(schema, crud.operations.UPDATE, data, filter)
		.then(function(result){
			//console.log(result);
			data[schema.primaryKey] = pk;
			return _transformDatum(schema, data);
		});
}

function _destroy(crud, schema, filter) {
	return crud.execute(schema, crud.operations.DELETE, null, filter);
}

module.exports = function(crudType, sqlConnInfo) {
	var crud = require(crudInterfaces[crudType])(sqlConnInfo);

	if(!crud) throw new Error(util.format("Invalid CRUD provider, %s.", crudType));

	return {
		//HTTP Exports
		get: _get.bind(null, crud),
		getOne: _getOne.bind(null, crud),
		putByPrimaryKey: _putByPrimaryKey.bind(null, crud),
		post: _post.bind(null, crud),
		delete: _delete.bind(null, crud),

		//CRUD Exports
		read: _read.bind(null, crud),
		one: _one.bind(null, crud),
		update: _update.bind(null, crud),
		create: _create.bind(null, crud),
		destroy: _destroy.bind(null, crud),

		//Testing interface
		_crud: crud
	};

};
