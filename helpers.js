var fs = require('fs');
var crypto = require('crypto');
var async = require("async");

var execVoidFunInLoopAsync = function(loopCount, fun, params, callback){	
	var counter = 0;
	loopCount = loopCount >= 0 ? loopCount : 0;
	params = params ? params : [];
	callback = callback ? callback : function(){};
	var loop = function(){
		fun.apply(params);
		if(counter++ < loopCount){
			setImmediate(loop);
		}else{
			process.nextTick(function(){
				callback();
			});
		}
	}
	loop();
}

var calculateDigest = function(algorithm, doc, encoding){
	var digest;
	var encoding = encoding ? encoding : 'hex';	
	try {
		var shasum = crypto.createHash(algorithm);
		shasum.update(doc);
		digest = shasum.digest(encoding); 
	}catch(e){
		console.log(e);
	}
	return digest;
}

var calculateMultiDigest = function(doc, algorithms, callback, loopCount){
	algorithms = algorithms ? algorithms : [];
	loopCount = loopCount ? loopCount : 0;
	callback = callback ? callback : function(){};
	if(algorithms.length <= 0){
		setImmediate(function(){
			callback("no digests algorithms set");
		});
		return;
	}else{
		var queueForAlgorithms = async.queue(function(method, callback){
				console.log("processing " + method);
				console.time(method);
				execVoidFunInLoopAsync(loopCount, 
					function(){
						calculateDigest(method,doc);
					},
					[],
					function() {
						console.timeEnd(method);
						callback(null, method + ": " + calculateDigest(method,doc));
					});						
			}, algorithms.length);			
		var digests = [];		
		for(var i = 0; i < algorithms.length; i++){	
			var method = algorithms[i];		
			console.log( method + " queued for processing...");
			queueForAlgorithms.push(method, function(err, digest){
				if(err) {
					digests.push(err);			
				} else {
					digests.push(digest);
				}
			});
							
		}
		queueForAlgorithms.drain = function(){		
			callback(null, digests);			
		};
	}
}

var hmac = function(algorithm, key, text, encoding) {	
	var hmac = crypto.createHmac(algorithm, key);
	return hmac.update(new Buffer(text)).digest(encoding);
}

var encode = function(obj, encoding) {
	var stringifyObj = JSON.stringify(obj);	
	return new Buffer(stringifyObj).toString(encoding);
}

var readJSONFile = function(fileName){
	if(!fs.existsSync(fileName)) {
		console.log("unable to open file: " + fileName);
		throw new Error("unable to open file: " + fileName);
	}
	var data = fs.readFileSync(fileName, {encoding:'utf8'});
	console.log(data);
	var object = JSON.parse(data);
	return object;	  	
}

exports.calculateDigest = calculateDigest; //= function(algorithm, text, encoding) e.g. calculateMultiDigest("md5", "some text", "hex")
exports.calculateMultiDigest = calculateMultiDigest; //= function(doc, algorithms, callback, loopCount)
exports.hmac = hmac; // = function(algorithm, key, text, encoding) 
exports.encode = encode; //encode = function(obj, encoding)
exports.readJSONFile = readJSONFile; // = function(fileName)
