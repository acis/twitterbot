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
            console.log("ENTRY:",entry);
            console.log("\n\n\n");

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
											user.createRelationshipTo(tweet, "tweets", function(err, rel){
												if(err) callback(err);

												console.log("CREATED REL: "+ user.data.screen_name + " "+ rel.type+" "+ node.data.id_str);
												callback(null, user, node);
											});

										});
                },

                function(user, tweet, callback){
                    if(entry.entities.hashtags.length>0){
                        console.log("HASHTAGS: ", entry.entities.hashtags);

												async.forEach(entry.entities.hashtags, function(ht, cb){

													var node = neodb.createNode({text: ht.text});
													insertOrUpdate(node, "Hashtag", "text", ht.text, function(err, hashtag){

														tweet.getRelationshipNodes({type: 'has_hashtag', direction: 'out'}, function(err, result){
															if(result.indexOf(hashtag)!=-1){
																console.log("TWEET "+ tweet.data.id_str+" CONTAINS " + hashtag.data.text);
																cb();
															} else {
																console.log("TWEET "+ tweet.data.id_str+" DOESN'T CONTAIN " + hashtag.data.text);
																tweet.createRelationshipTo(hashtag, "has_hashtag", function(err, rel){
																	if(err) cb(err);
																	console.log("CREATED REL: "+ tweet.data.id_str+" "+ rel.type +" "+ hashtag.data.text);
																	cb();
																});
															}
														});

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
                        neodb.getIndexedNode("Tweet", "id_str", entry.in_reply_to_status_id_str, function(err,inreplyto){

                            if(inreplyto) {
                                console.log("IN REPLY TO TWEET EXITS: ", inreplyto.data.id_str);
                                //check relationship
                                inreplyto.getRelationshipNodes({type: 'replies_to', direction: 'in'}, function(err, result){
                                    if(result.indexOf(tweet)!=-1){
                                        console.log("TWEET "+ tweet.data.id_str+" IS REPLY TO " + inreplyto.data.id_str);
                                        callback(null, user, tweet);
                                    } else {
                                        console.log("TWEET "+ tweet.data.id_str+" ISNT'T REPLY TO " + inreplyto.data.id_str);
                                        tweet.createRelationshipTo(inreplyto, "replies_to", function(err, rel){
                                            if(err) callback(err);
                                            console.log("CREATED REL: "+ tweet.data.id_str+" "+ rel.type +" "+ inreplyto.data.id_str);
                                            callback(null, user, tweet);
                                        });
                                    }
                                });

                            } else {
                                //check if reply user exists
                                neodb.getIndexedNode("User", "screen_name", entry.in_reply_to_screen_name, function(err,replytouser){

                                    if(replytouser) {
                                        console.log("USER EXITS: ", replytouser.data.screen_name);
                                        var replytweet = neodb.createNode({id_str: entry.in_reply_to_status_id_str});
                                        replytweet.save(function (err, node) {
                                            if (err)  callback(err);
                                            else {
                                                node.index("Tweet", "id_str", entry.in_reply_to_status_id_str);
                                                console.log("CREATED REPLY TWEET ", node.data.id_str );
                                                replytouser.createRelationshipTo(node, "tweets", function(err, rel){
                                                    if(err) callback(err);
                                                    console.log("CREATED REL: "+ replytouser.data.screen_name + " "+ rel.type+" "+ node.data.id_str);
                                                    node.createRelationshipTo(tweet, "replies_to", function(err, rel){
                                                        if(err) callback(err);
                                                        console.log("CREATED REL: "+ node.data.id_str+" "+ rel.type +" "+ tweet.data.id_str);
                                                        callback(null, user, tweet);
                                                    });
                                                });

                                            }
                                        });

                                    } else {
                                        console.log("USER NOT EXIST: ", entry.in_reply_to_screen_name);
                                        var user = neodb.createNode({id_str:entry.in_reply_to_user_id_str, screen_name:entry.in_reply_to_screen_name});
                                        user.save(function (err, replytouser) {

                                            if (err) {
                                                console.log(err);
                                                callback(err);
                                            } else {
                                                replytouser.index("User", "screen_name", entry.in_reply_to_screen_name, false);
                                                console.log(" CREATED USER: ", replytouser.data.screen_name);
                                                var replytweet = neodb.createNode({id_str: entry.in_reply_to_status_id_str});
                                                replytweet.save(function (err, replytweet) {
                                                    if (err) {
                                                        console.log(err);
                                                        callback(err);
                                                    } else {
                                                        replytweet.index("Tweet", "id_str", entry.in_reply_to_status_id_str, false);
                                                        console.log("CREATED IN REPLY TO TWEET ", replytweet.data.id_str );
                                                        replytouser.createRelationshipTo(replytweet, "tweets", function(err, rel){
                                                            if(err) callback(err);
                                                            console.log("CREATED REL: "+ replytouser.data.screen_name + " "+ rel.type+" "+ replytweet.data.id_str);
                                                            tweet.createRelationshipTo(replytweet, "replies_to", function(err, rel){
                                                                if(err) callback(err);
                                                                console.log("CREATED REL: "+ replytweet.data.id_str+" "+ rel.type +" "+ tweet.data.id_str);
                                                                callback(null, user, tweet);
                                                            });
                                                        });

                                                    }
                                                });
                                            }
                                        });
                                    }

                                });
                            }

                        });


                    } else  {
                        callback(null, user, tweet);
                    }

                },

                function(user, tweet, callback){
                    if(entry.entities.user_mentions.length>0) {
                        console.log(entry.entities.user_mentions);
                        async.forEach(entry.entities.user_mentions, function(um, cb){
                            neodb.getIndexedNode("User", "screen_name", um.screen_name, function(err,mentioned){
                                console.log("MENTIONED ", um.screen_name);

                                if(mentioned){
                                    console.log("MENTIONED USER EXISTS ", mentioned.data.screen_name);
                                    user.getRelationshipNodes({type: 'mentions', direction: 'out'}, function(err, result){
                                        if(result.indexOf(mentioned)!=-1){
                                            console.log("USER "+ user.data.screen_name+" MENTIONED " + mentioned.data.screen_name);
                                            cb();
                                        } else {
                                            console.log("USER "+user.data.screen_name+" DOESN'T MENTION " + mentioned.data.screen_name);
                                            tweet.createRelationshipTo(mentioned, "mentions", function(err, rel){
                                                if(err) cb(err);
                                                console.log("CREATED REL: "+ user.data.screen_nam+" "+ rel.type +" "+ mentioned.data.screen_name);
                                                cb();
                                            });
                                        }
                                    });
                                } else {
                                    console.log("MENTIONED USER DOESN'T EXISTS ", um.screen_name);
                                    var node = neodb.createNode({id_str:um.id_str, name: um.name, screen_name:um.screen_name});
                                    node.save(function (err, mentioned) {
                                        if (err) cb(err);
                                        else {
                                            console.log("CREATED MENTIONED "+ mentioned.data.screen_name);
                                            mentioned.index("User", "screen_name", mentioned.screen_name, false);
                                            tweet.createRelationshipTo(mentioned, "mentions", function(err, rel){
                                                if(err) cb(err);
                                                console.log("CREATED REL: "+ user.data.screen_name+" "+ rel.type +" "+ mentioned.data.screen_name);
                                                cb();
                                            })
                                        }
                                    });
                                }
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
			callback(null, result);
		} else {
			console.log(type+" DOES NOT EXIT: ", indexkey,": ", indexvalue);

			node.save(function (err, saved) {
				if (err) {
					callback(err);
				}	else {
					console.log("CREATED " +type+" : ", indexkey,": ", indexvalue);
					saved.index("User", "screen_name", indexvalue, false);
					callback(null, saved);
				}
			});
		}

	});


}

