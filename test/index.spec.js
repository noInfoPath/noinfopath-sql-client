var assert = require("assert"),
	sqlConnInfo = require("./fixtures/sql-conn-info"),
	sqlClientInit = require("../index"),
	testSchema = require("./fixtures/test-schema"),
	testData = require("./fixtures/test-data"),
	sqlClient = sqlClientInit("mysql", sqlConnInfo);


describe("Testing noinfopath-sql-client E2E", function () {
	describe("Testing initialization", function () {

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

	describe("Testing direct mysql crud actions", function () {
		var crud = sqlClient._crud;

		it("should create a new record", function (done) {
			crud.execute(testSchema, crud.operations.CREATE, testData)
				.then(function (r) {
					assert(r.affectedRows > 0);
					testData.id = r.insertId;
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should update existing record", function (done) {
			testData.data += " " + (new Date()).toLocaleString();

			crud.execute(testSchema, crud.operations.UPDATE, testData)
				.then(function (r) {
					assert(r.affectedRows > 0);
					//console.log(r);
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
					//console.log(r);
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

			crud.execute(testSchema, crud.operations.DELETE, null, f)
				.then(function (r) {
					assert.ok(r);
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("TODO: should delete records using OData query", function () {

		});

	});

	describe("Testing NoRest driven CRUD operations", function(){
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
			testData.data += " " + (new Date()).toLocaleString();

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
					//console.log(r);
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
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		xit("TODO: should delete records using OData query", function () {

		});

	});

	describe("Testing route driven CRUD operations", function(){
		it("should create a new record", function (done) {
			var req = {
				body: testData
			};

			sqlClient.post(testSchema, req)
				.then(function (r) {
					testData = r;
					assert.ok(r);
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		it("should update existing record", function (done) {
			testData.data += " " + (new Date()).toLocaleString();

			var req = {
				body: testData
			};
			//console.log(testData);

			sqlClient.putByPrimaryKey(testSchema, req)
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
			var req = {};

			sqlClient.get(testSchema, req)
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
			var req = {
				params: {
					id: testData.id
				}
			};

			sqlClient.getOne(testSchema, req)
				.then(function (r) {
					assert.ok(r);
					//console.log(r);
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
					//console.log(r);
					done();
				})
				.catch(function (e) {
					console.error(e);
					done(e);
				});
		});

		xit("TODO: should delete records using OData query", function () {

		});
	});

	describe("Testing misc. functions", function(){
		it("should have a wrapSchema function", function(){
			assert(sqlClient.wrapSchema);
		});

		describe("should have a wrapSchema should return a CRUD inteface specific to provided schema.", function(){
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
						//console.log(r);
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
						//console.log(r);
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
						//console.log(r);
						done();
					})
					.catch(function (e) {
						console.error(e);
						done(e);
					});
			});

			xit("TODO: should delete records using OData query", function () {

			});

		});
	})
});
