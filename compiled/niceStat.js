/* #declare require, module, Date; */   

var fs = require('fs');

var statsCache = {};

module.exports = function (path, next) {
	if (statsCache[path] && statsCache[path].expires > new Date().getTime()) {
		next(null, statsCache[path].stats);
		return;
	}
	
	fs.stat(path, onStats);
	
	function onStats(__err, stats) { if (__err) { next(__err); return; }
		statsCache[path] = {
			stats: stats,
			expires: (new Date().getTime() + 1500)
		};
		
		next(null, stats);
	}
};
