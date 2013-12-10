var	request = require('request');
var	JSONStream = require('JSONStream');
var	OAuth = require('oauth');
var	neo4j= require('neo4j');
var async = require('async');
var mysql = require('mysql');
var datejs = require('datejs');
var	keys = require('./keys.js');



//var userArray = ['SarahBrownUK', 'denverfoodguy', 'BrianBrownNet', 'RichardPBacon', 'eddieizzard', 'stephenfry', 'umairh', 'rustyrockets', 'tinchystryder', 'HilaryAlexander', 'Zee', 'jemimakiss', 'RichardDawkins' ];
var neodb;
var mysqlConn;
var bearerToken;
var sql = "UPDATE users SET name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https=?, url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?";
var userFields = ["name","description", "created_at", "location","profile_image_url","profile_image_url_https","url","listed_count","favourites_count", "followers_count", "statuses_count","friends_count"];



function init(){

	neodb = new neo4j.GraphDatabase('http://localhost:7474');

	mysqlConn = mysql.createConnection({
		host     : keys.mysql_host,
		database : keys.mysql_db,
		user     : keys.mysql_username,
		password : keys.mysql_pwd
	});
	mysqlConn.connect();

	var OAuth2 = OAuth.OAuth2;
	var oauth2 = new OAuth2(keys.consumer_key, keys.consumer_secret, 'https://api.twitter.com/', null, 'oauth2/token', null);
	oauth2.getOAuthAccessToken('', {'grant_type': 'client_credentials'}, function(e, access_token, refresh_token, results) {
			bearerToken = access_token;
			var userArray =[];
			mysqlConn.query('SELECT screen_name FROM users order by created_at asc;', function(err, result) {
				if(err) console.log(err);

				result.forEach(function(user){
					userArray.push(user.screen_name);
				});
				getData(userArray);
			});


		}
	);
}

function getData(userArray){

	async.forever(function(next){
		var user = userArray.shift();
		console.log(user);
		mysqlConn.query('SELECT created_at FROM users WHERE screen_name=?', user, function(err, result) {
			if(err) console.log(err);
			if(result[0].created_at == null) {
				getUserInfo({screen_name:user, include_entities:false}, function(err){
					if(err) console.log(err);

				});
			}
		});

		console.log(user);

		getTweets({screen_name:user, count:200}, function(){
			userArray.push(user);
			setImmediate(next);
		});

	}, function(err){
		console.log(err);
	});

}

function getUserInfo(params, callback){
	var stream = JSONStream.parse();
	var options ={
		url: "https://api.twitter.com/1.1/users/show.json?",
		qs: params,
		headers: {
			'User-Agent': 'AIC Data Mining',
			Authorization: "Bearer " + bearerToken
		}
	};
	request(options).pipe(stream);
	stream.on('root', function(obj){
		var values=[];
		userFields.forEach(function(field){
			if(field === "created_at")
				values.push(new Date(Date.parse(obj[field])));
			else values.push(obj[field]);
		});

		values.push(obj.id_str);

		mysqlConn.query(sql, values, function(err, result) {
			if(err) console.log(err);
		});

	});

}


function getTweets(params, reqcallback) {
	var stream = JSONStream.parse();
	var options ={
		url: "https://api.twitter.com/1.1/statuses/user_timeline.json?",
		qs: params,
		headers: {
			'User-Agent': 'AIC Data Mining',
			Authorization: "Bearer " + bearerToken
		}
	};

	request(options).pipe(stream);
	stream.on('root', function(obj){
		async.eachSeries(obj, function(entry, entrycallback){

			async.waterfall([
				function(callback){
					var node = neodb.createNode({id_str:entry.user.id_str, name: entry.user.name, screen_name:entry.user.screen_name});
					insertOrUpdate(node, "User","screen_name", entry.user.screen_name, function(err, user){
						callback(err, user);
					});
				},

				function(user, callback){
					var node = neodb.createNode({id_str: entry.id_str, text: entry.text, retweet_count:entry.retweet_count, favorite_count:entry.favorite_count});
					insertOrUpdate(node, "Tweet", "id_str", entry.id_str, function(err, tweet){

						createRelationship(user, tweet, "tweets", function(err){
							callback(err, user, tweet);
						});
					});
				},

				function(user, tweet, callback){
					if(entry.entities.hashtags.length>0){
						async.forEach(entry.entities.hashtags, function(ht, cb){

							var node = neodb.createNode({text: ht.text});
							insertOrUpdate(node, "Hashtag", "text", ht.text, function(err, hashtag){
								createRelationship(tweet, hashtag, "has_hashtag", function(err){
									cb(err);
								})
							});
						}, function(err){
							callback(err, user, tweet);
						});
					} else {
						callback(null, user, tweet);
					}
				},

				function(user, tweet, callback){
					if(entry.in_reply_to_status_id){

						var node = neodb.createNode({id_str:entry.in_reply_to_user_id_str, screen_name:entry.in_reply_to_screen_name});
						insertOrUpdate(node, "User", "screen_name",entry.in_reply_to_screen_name,  function(err, replyuser){
							if(err) callback(err);
							var node = neodb.createNode({id_str: entry.in_reply_to_status_id_str});

							insertOrUpdate(node, "Tweet", "id_str", entry.in_reply_to_status_id_str, function(err, replytweet){
								if(err) callback(err);
								createRelationship(replyuser, replytweet, "tweets", function(err){
									if(err) callback(err);
									createRelationship(tweet, replytweet, "in_reply_to", function(err){
										callback(err, user, tweet);
									})
								});
							});
						});

					} else  {
						callback(null, user, tweet);
					}

				},

				function(user, tweet, callback){
					if(entry.entities.user_mentions.length>0) {
						async.forEach(entry.entities.user_mentions, function(um, cb){
							var node = neodb.createNode({id_str:um.id_str, name: um.name, screen_name:um.screen_name});
							insertOrUpdate(node, "User", "screen_name",um.screen_name,function(err, mentioned){

								createRelationship(tweet, mentioned, "mentions", function(err){
									cb(err);
								})
							});

						}, function(err){
							if(err) {callback(err);}
							callback(null, user, tweet);
						});
					} else {
						callback(null, user, tweet);
					}

				}

			], function (result, err) {
				entrycallback();
			});


		}, function(err){
			if(err) console.log("ERROR: "+ err);
			reqcallback();
		});
	});




}



//var sql = "INSERT OR UPDATE users SET id_str =?, screen_name=?, name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https,url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?";
//var userFields = ["id_str", "screen_name", "name","description", "created_at", "location","profile_image_url","profile_image_url_https","url","listed_count","favourites_count", "followers_count", "statuses_count","friends_count"];



function insertOrUpdate(node, type, indexkey, indexvalue, callback){
	neodb.getIndexedNode(type, indexkey, indexvalue, function(err,result){
		if(result) {
			var modified=false;
			for (var i in node.data){
				if(!result.data[i] || result.data[i]!=node.data[i]){
					result.data[i]=node.data[i];
					modified=true;
				}
			}
			if(modified){
				result.save(function (err, saved) {
					if (err) {
						callback(err);
					}	else {
						saved.index(type, indexkey, indexvalue, false);
						callback(null, saved);
					}
				});
			} else callback(null, result);
		} else {
			node.save(function (err, saved) {
				if (err) {
					callback(err);
				} else {
					saved.index(type, indexkey, indexvalue, false);
					if(type=="User"){
						/*var query = mysqlConn.query('INSERT INTO users SET ?;', node.data, function(err, result) {
							if(err) console.log(err);
						});
						console.log(query.sql); */
					}
					callback(null, saved);
				}
			});
		}
	});
}

function createRelationship(from, to, type, callback){
	from.path(to, type, 'out',1,'shortestPath', function(err, result){

		if(result){
			//console.log("REL EXISTS ", type);
			callback(err);
		} else {
			//console.log("REL DOESN'T EXIST ", type);
			from.createRelationshipTo(to, type, function(err, rel){
				//console.log("REL CREATED ");
				callback(err);
			});
		}
	});

}

init();


