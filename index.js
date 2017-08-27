var noRestInit = require("./no-rest")
	;

module.exports = function(crudType, sqlConnInfo) {
	return noRestInit(crudType, sqlConnInfo);
};
