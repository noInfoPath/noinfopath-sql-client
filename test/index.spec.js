var assert = require("assert"),
	sqlConnInfo = require("./fixtures/sql-conn-info"),
	sqlClientInit = require("../index"),
	testSchema = require("./fixtures/test-schema"),
	testStoredProcedure = require("./fixtures/test-stored-procedure"),
	testReadSP = require("./fixtures/test-read-sp"),
	testWriteSP = require("./fixtures/test-write-sp"),
	testPerson = require("./fixtures/test-person"),
	testGetCompanySP = require("./fixtures/test-get-company-sp"),
	testData = require("./fixtures/test-data"),
	sqlClient = sqlClientInit("mysql", sqlConnInfo),
	_newId;


describe("Testing noinfopath-sql-client E2E", function () {
	describe("initialization", function () {

		it("should have loaded in the initialization function.", function () {
			assert(typeof (sqlClientInit) === "function");
		});

		it("should initialize as `mysql` crudType", function () {
			assert.ok(sqlClient, "sqlClient initialized.");
		});

		it("should have sqlClient._crud test interface.", function () {
			assert.ok(sqlClient._crud);
		});

		it("should have initialized with sqlClient.crudType of `mysql`", function () {
			assert(sqlClient._crud.crudType === "mysql");
		});
	});

	describe("Direct mysql crud actions", function () {
		var crud = sqlClient._crud;

		it("should create a new record", function (done) {
			crud.execute(testSchema, crud.operations.CREATE, testData)
				.then(function (r) {
					assert(r.affectedRows > 0);
					_newId = r.insertId;
					testData.id = _newId;
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should update existing record", function (done) {
			testData.data = "CRUD Direct Test Update " + (new Date()).toLocaleString();
			crud.execute(testSchema, crud.operations.UPDATE, testData)
				.then(function (r) {
					assert(r.affectedRows > 0);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read all existing record", function (done) {
			crud.execute(testSchema, crud.operations.READ)
				.then(function (r) {
					assert(r.length > 0);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read one existing record using route params", function (done) {
			crud.execute(testSchema, crud.operations.READ, null, {
					url: "/",
					params: {
						id: _newId
					}
				})
				.then(function (r) {
					assert(r.length === 1);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read one existing record using plain querystring", function (done) {
			crud.execute(testSchema, crud.operations.READ, null, {
					url: "/",
					query: {
						id: _newId
					}
				})
				.then(function (r) {
					assert(r.length === 1);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read one existing record using plain javascript object", function (done) {
			crud.execute(testSchema, crud.operations.READ, null, {id: _newId})
				.then(function (r) {
					console.log(r);
					assert(r.length === 1);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read one existing record using plain ODATA $filter", function (done) {
			var f = {
				url: "/",
				odata: {
					query: {
						"select": "*",
						"where": "?? = ?",
						"orderby": "",
						"parameters": ["id", _newId]
					}
				}
			};
			crud.execute(testSchema, crud.operations.READ, null, f)
				.then(function (r) {
					assert(r.length === 1);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should delete the last record written using route parameter", function (done) {
			var f = {
				url: "/",
				params: {
					id: _newId
				}
			};

			crud.execute(testSchema, crud.operations.DELETE, null, f)
				.then(function (r) {
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

	});

	describe("NoRest driven CRUD operations", function () {
		it("should create a new record", function (done) {

			sqlClient.create(testSchema, testData)
				.then(function (r) {
					testData = r;
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should update existing record", function (done) {
			testData.data = "NoRest driven test update " + (new Date()).toLocaleString();

			sqlClient.update(testSchema, testData)
				.then(function (r) {
					testData = r;
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read all existing record", function (done) {
			sqlClient.read(testSchema)
				.then(function (r) {
					assert(r.length > 0);
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read the last record written", function (done) {
			sqlClient.one(testSchema, testData.id)
				.then(function (r) {
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should delete the last record written using route parameter", function (done) {
			var f = {
				params: {
					id: testData.id
				}
			};

			sqlClient.destroy(testSchema, f)
				.then(function (r) {
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

	});

	describe("Stored Procedure Calls", function () {
		it("should save (upsert) to the test table using a stored procedure", function (done) {
			sqlClient.writeSP(testWriteSP.route, testWriteSP.payload)
				.then(function (r) {
					_newId = r.id;
					assert(r);
					assert(r.id);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				})
		});

		it("should read one record from the test table using a stored procedure", function (done) {
			var f = {
				url: "/",
				params: {
					"id": _newId
				}
			};

			sqlClient.readSP(testReadSP.routeParam, null, f)
				.then(function (r) {
					assert(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		xit("sp_sop_person_address should save to all tables", function (done) {
			sqlClient.writeSP(testPerson.route, testPerson.data)
				.then(function (r) {
					assert(true);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		xit("sp_sop_get_company should get a record based on an id", function (done) {
			sqlClient.readSP(testGetCompanySP.route, testGetCompanySP.readIDData)
				.then(function (r) {
					assert(r.length > 0);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		xit("sp_sop_get_company should get a record based on an email", function (done) {
			sqlClient.readSP(testGetCompanySP.route, testGetCompanySP.readEmailData)
				.then(function (r) {
					console.log(r[0]);
					assert(r.length > 0);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});
	})

	describe("Testing route driven CRUD operations", function () {
		it("should create a new record", function (done) {
			var req = {
				body: testData
			};

			sqlClient.post(testSchema, req)
				.then(function (r) {
					testData = r;
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should update existing record", function (done) {
			testData.data = "Test " + (new Date()).toLocaleString();

			var data = testData.data,
				req = {
				body: testData
			};

			sqlClient.putByPrimaryKey(testSchema, req)
				.then(function (r) {
					console.log(r.data, testData.data);
					assert(r.data === testData.data);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read all existing record", function (done) {
			//var req = {};

			sqlClient.get(testSchema)
				.then(function (r) {
					assert(r.length > 0);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should read the last record written", function (done) {
			var req = {
				url: "/",
				params: {
					id: testData.id
				}
			};

			sqlClient.getOne(testSchema, req)
				.then(function (r) {
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should delete the last record written using route parameter", function (done) {
			var req = {
				params: {
					id: testData.id
				}
			};

			sqlClient.delete(testSchema, req)
				.then(function (r) {
					assert.ok(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

	});

	describe("Testing misc. functions", function () {
		it("should have a wrapSchema function", function () {
			assert(sqlClient.wrapSchema);
		});

		describe("should have a wrapSchema should return a CRUD inteface specific to provided schema.", function () {
			var wrapped = sqlClient.wrapSchema(testSchema);
			it("should create a new record", function (done) {

				wrapped.create(testData)
					.then(function (r) {
						testData = r;
						assert.ok(r);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

			it("should update existing record", function (done) {
				testData.data += " " + (new Date()).toLocaleString();

				wrapped.update(testData)
					.then(function (r) {
						testData = r;
						assert.ok(r);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

			it("should read all existing record", function (done) {
				wrapped.read()
					.then(function (r) {
						assert(r.length > 0);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

			it("should read the last record written", function (done) {
				wrapped.one(testData.id)
					.then(function (r) {
						assert.ok(r);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

			it("should delete the last record written using route parameter", function (done) {
				var f = {
					params: {
						id: testData.id
					}
				};

				wrapped.destroy(f)
					.then(function (r) {
						assert.ok(r);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

		});
	})
});
