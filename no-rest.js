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
			if (typeof (n) === "string") {
				n = n.replace(/ /gi, "T");
				n = n.indexOf("Z") === -1 ? n + "Z" : n;
			}

			return n ? n : null;
		},
		"datetime2": function (n) {
			if (typeof (n) === "string") {
				n = n.replace(/ /gi, "T");
				n = n.indexOf("Z") === -1 ? n + "Z" : n;
			}

			return n ? n : null;
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

			if (!col) throw new Error(util.format("%s is not a column in the data table.  Check  your spelling and try again.", p));

			cfn = conversionFunctionsIn[col.type];

			if (cfn) {
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
	if (!schema.columns) return datum;

	for (var p in datum) {
		try {
			var prop = datum[p],
				col = schema.columns[p],
				cfn;

			if (!col) throw new Error(util.format("%s is not a column in the data table.  Check  your spelling and try again.", p));

			cfn = conversionFunctionsOut[col.type];

			if (cfn) {
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
			delete b._id;
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
	return _read(crud, schema, req);
}

function _getOne(crud, schema, req) {
	return _one(crud, schema, req); //TODO: make this support ODATA filter also.
}

function _putByPrimaryKey(crud, schema, req) {

	var filter = {},
		data = _scrubDatum(schema, _resolveBodyData(req).body),
		routeID = (req.params && req.params.id) || data[schema.primaryKey];

	filter[schema.primaryKey] = routeID;

	return _update(crud, schema, data, filter);

}

function _post(crud, schema, req) {
	var data = schema.storageType === "gcs" ? req : req.body;
	return _create(crud, schema, data)
		.then(function (result) {
			return result || data; //??? what does mydata do?
		});
}

function _delete(crud, schema, req) {
	return _destroy(crud, schema, req); //TODO: need to resolve filter from req here.
}

//CRUD Aliases
function _create(crud, schema, data) {
	if (data._id) delete data._id; //This is a mongodb thing.

	if (data[schema.primaryKey]) delete data[schema.primaryKey];

	if (schema.storageType !== "gcs") {
		data = _scrubDatum(schema, data);
	}

	return crud.execute(schema, crud.operations.CREATE, data)
		.then(function (result) {
			if (schema.storageType === "gcs") {
				return result;
			} else {
				data[schema.primaryKey] = result.insertId;

				var r = _transformDatum(schema, data);
				return r;
			}
		});
}

function _read(crud, schema, filter) {
	return crud.execute(schema, crud.operations.READ, null, filter)
		.then(function (results) {

			if (results.value) {
				results.value = results.value.map(function (datum) {
					return _transformDatum(schema, datum);
				});
			} else {
				if (Array.isArray(results)) {
					results = results.map(function (datum) {
						return _transformDatum(schema, datum);
					});
				} else {
					return results;
				}
			}

			return results;
		});
}

function _readSP(crud, schema, payload, filter) {
	return crud.execute(schema, crud.operations.EXECSP, payload, filter)
		.then(function (results) {
			results = results.map(function (datum) {
				return _transformDatum(schema, datum);
			});

			if (schema.activeUri && schema.activeUri.returnOne) {
				return results[0]; //Always returns the first result item or undefined.
			} else {
				return results;
			}

			return results;
		});
}

function _writeSP(crud, schema, payload, filter) {
	return _readSP(crud, schema, payload, filter)
		.then(function (results) {
			return results.length > 0 ? results[0] : results;
		});
}

function _one(crud, schema, filter) {
	return _read(crud, schema, filter)
		.then(function (results) {
			return results.length > 0 ? _transformDatum(schema, results[0]) : null;
		});
}

function _update(crud, schema, data, filter) {
	return crud.execute(schema, crud.operations.UPDATE, data, filter)
		.then(function (result) {
			return _transformDatum(schema, data);
		});
}

function _destroy(crud, schema, filter) {
	return crud.execute(schema, crud.operations.DELETE, null, filter);
}

function _wrapSchema(crud, schema) {
	return {
		read: _read.bind(null, crud, schema),
		readSP: _readSP.bind(null, crud, schema),
		one: _one.bind(null, crud, schema),
		update: _update.bind(null, crud, schema),
		create: _create.bind(null, crud, schema),
		writeSP: _writeSP.bind(null, crud, schema),
		destroy: _destroy.bind(null, crud, schema),
		get: _get.bind(null, crud, schema),
		getOne: _getOne.bind(null, crud, schema),
		putByPrimaryKey: _putByPrimaryKey.bind(null, crud, schema),
		post: _post.bind(null, crud, schema),
		delete: _delete.bind(null, crud, schema)
	};
}

module.exports = function (crudType, sqlConnInfo) {

	var crud;

	console.log("crudType", typeof (crudType));


	if (typeof (crudType) === "string") {
		crud = require(crudInterfaces[crudType])(sqlConnInfo);
	} else if (typeof (crudType) === "object") {
		crud = crudType
	} else {
		throw new Error("Unsupported crudType");
	}

	if (!crud) throw new Error(util.format("Invalid CRUD provider, %s.", crudType));

	return {
		//HTTP Exports
		get: _get.bind(null, crud),
		getOne: _getOne.bind(null, crud),
		putByPrimaryKey: _putByPrimaryKey.bind(null, crud),
		post: _post.bind(null, crud),
		delete: _delete.bind(null, crud),

		//CRUD Exports
		read: _read.bind(null, crud),
		readSP: _readSP.bind(null, crud),
		writeSP: _writeSP.bind(null, crud),
		one: _one.bind(null, crud),
		update: _update.bind(null, crud),
		create: _create.bind(null, crud),
		destroy: _destroy.bind(null, crud),

		//Testing interface
		_crud: crud,

		wrapSchema: _wrapSchema.bind(null, crud)
	};

};
