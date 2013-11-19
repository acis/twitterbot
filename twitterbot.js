var	request = require('request');
var	JSONStream = require('JSONStream');
var	OAuth = require('oauth');
var	neo4j= require('neo4j');
var async = require('async');
var	keys = require('./keys.js');


var neodb = new neo4j.GraphDatabase('http://localhost:7474');
var stream = JSONStream.parse();
var bearerToken;


function getBearerToken(){
	var OAuth2 = OAuth.OAuth2;
	var oauth2 = new OAuth2(keys.consumer_key, keys.consumer_secret, 'https://api.twitter.com/', null, 'oauth2/token', null);
	oauth2.getOAuthAccessToken('', {'grant_type': 'client_credentials'}, function(e, access_token, refresh_token, results) {
				bearerToken = access_token;
				getTweets();
			}
	);
}

function getTweets(){

	///think up a smart algorithm for traversing tweets and make requests
	var path='/1.1/statuses/user_timeline.json?';
	var params={screen_name:'ConanOBrien', count:3};
    //var params={screen_name:"aic_64", count:3};
	makeRequest(path, params);

}



function makeRequest(path, params) {

	var options ={
		url: "https://api.twitter.com"+path,
		qs: params,
		headers: {
			'User-Agent': 'AIC Data Mining',
			Authorization: "Bearer " + bearerToken
		}
	};

	request(options).pipe(stream);
	stream.on('root', function(obj){

		async.eachSeries(obj, function(entry, entrycallback){
			console.log("\n\n\nENTRY:",entry);
            console.log("\n\n");


			async.waterfall([
				function(callback){
					var node = neodb.createNode({id_str:entry.user.id_str, name:entry.user.name, screen_name:entry.user.screen_name});
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
						console.log("HASHTAGS: ", entry.entities.hashtags);

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
						insertOrUpdate(node, "User", "screen_name",entry.in_reply_to_screen_name, function(err, replyuser){
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
						console.log(entry.entities.user_mentions);
						async.forEach(entry.entities.user_mentions, function(um, cb){
							var node = neodb.createNode({id_str:um.id_str, name: um.name, screen_name:um.screen_name});
							insertOrUpdate(node, "User", "screen_name",um.screen_name, function(err, mentioned){
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
		});


	});


}

getBearerToken();

function insertOrUpdate(node, type, indexkey, indexvalue, callback){
    neodb.getIndexedNode(type, indexkey, indexvalue, function(err,result){
		if(result) {
			console.log(type+" EXITS: ", result.data[indexkey]);
            var modified=false;
            for (var i in node.data){
                if(!result.data[i] || result.data[i]!=node.data[i]){
                  console.log("ADDING OR UPDATING PROPERTY", i );
                  console.log("FROM ",result.data[i], "TO", node.data[i] );
                  result.data[i]=node.data[i];
                  modified=true;
                }
            }
            if(modified){
                result.save(function (err, saved) {
                    if (err) {
                        callback(err);
                    }	else {
                        console.log("UPDATED " +type+" : ", indexkey,": ", indexvalue);
                        saved.index(type, indexkey, indexvalue, false);
                        callback(null, saved);
                    }
                });
            } else callback(null, result);
		} else {
			console.log(type+" DOES NOT EXIT: ", indexkey,": ", indexvalue);

			node.save(function (err, saved) {
				if (err) {
					callback(err);
				}	else {
					console.log("CREATED " +type+" : ", indexkey,": ", indexvalue);
					saved.index(type, indexkey, indexvalue, false);
					callback(null, saved);
				}
			});
		}
	});
}

function createRelationship(from, to, type, callback){
	from.path(to, type, 'out',1,'shortestPath', function(err, result){

		if(result){
			console.log("REL EXISTS ", from.data, type, to.data);
			callback(err);
		} else {
			console.log("REL DOESN'T EXIST ", from.data, type, to.data);
			from.createRelationshipTo(to, type, function(err, rel){
				console.log("REL CREATED ");
				callback(err);
			});
		}
	});

}


