/* #declare exports, Math; */  
/* #include undef; */ 

// maxint for 32-bits:      4,294,967,295
// maxint for 6 digits:    56,800,235,583
// maxint for 7 digits: 3,521,614,606,207

var base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
var base62CharsArray = [];
var base62CharsMap = {};
(function () {
	var i;
	for (i = 0; i < base62Chars.length; i += 1) {
		base62CharsArray[i] = base62Chars.substr(i, 1);
		base62CharsMap[base62Chars.substr(i, 1)] = i;
	}
})();

exports.toBase62 = function (num, padding) {
	var i;
	var digits = [];
	if (!undef(padding)) {
		for (i = 0; i < padding; i += 1) {
			digits[i] = 0;
		}
	}
	
	var place = 0, mod;
	while (num > 0) {
		mod = num % 62;
		num = Math.floor(num / 62);
		digits[place] = mod;
		place += 1;
	}
	
	var str = '';
	for (i = digits.length - 1; i >= 0; i -= 1) {
		str += base62CharsArray[digits[i]];
	}
	
	return str;
};

exports.fromBase62 = function (str) {
	var num = 0;
	var place = 0;
	var i;
	for (i = str.length-1; i >= 0; i -= 1) {
		num += base62CharsMap[str.substr(i, 1)] * Math.pow(62, place);
		place += 1;
	}
	return num;
};

function undef(x) {
	return typeof x === 'undefined';
}
