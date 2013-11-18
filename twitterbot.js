var https = require('https');
var	qs = require('qs');
var	request = require('request');
var	JSONStream = require('JSONStream');
var	OAuth = require('oauth');
var	neo4j= require('neo4j');git
var	es = require('event-stream');
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
                    neodb.getIndexedNode("User", "screen_name", entry.user.screen_name, function(err,usernode){

                        if(usernode) {
                            console.log("USER EXITS: ", usernode.data.screen_name);
                            callback(null, usernode);
                        } else {
                            console.log("USER NOT EXIT: ", entry.user.screen_name);
                            var user = neodb.createNode({id_str:entry.user.id_str, name:entry.user.name,screen_name:entry.user.screen_name});
                            user.save(function (err, node) {
                                if (err) callback(err);
                                else {
                                    node.index("User", "screen_name", entry.user.screen_name);
                                    console.log(" CREATED USER: ", node.data.screen_name);
                                    callback(null, node);
                                }
                            });
                        }

                    });
                },

                function(user, callback){
                    neodb.getIndexedNode("Tweet", "id_str", entry.id_str, function(err, tweetnode){
                        if(tweetnode){
                            console.log("TWEET EXITS: ", tweetnode.data.id_str);
                            callback(null, user, tweetnode);
                        } else {
                            console.log("TWEET DOES NOT EXIST: ", entry.id_str);
                            var tweet = neodb.createNode({id_str: entry.id_str, text: entry.text, retweet_count:entry.retweet_count, favorite_count:entry.favorite_count});
                            tweet.save(function (err, node) {
                                if (err)  callback(err);
                                else {
                                    node.index("Tweet", "id_str", entry.id_str);
                                    console.log("CREATED TWEET ", node.data.id_str );
                                    user.createRelationshipTo(node, "tweets", function(err, rel){
                                        if(err) callback(err);
                                        console.log("CREATED REL: "+ user.data.screen_name + " "+ rel.type+" "+ node.data.id_str);
                                        callback(null, user, node);
                                    });

                                }
                            });

                        }

                    });

                },

                function(user, tweet, callback){
                    console.log("LOOKING AT HASHTAGS");
                    if(entry.entities.hashtags.length>0){
                        console.log("HASHTAGS: ", entry.entities.hashtags);
                        async.forEach(entry.entities.hashtags, function(ht, cb){
                            neodb.getIndexedNode("Hashtag", "text", ht.text, function(err,hashtagnode){
                                console.log("HASHTAG ", ht.text);

                                if(hashtagnode){
                                    console.log("HASHTAG EXISTS ", hashtagnode.data.text);
                                    tweet.getRelationshipNodes({type: 'has_hashtag', direction: 'out'}, function(err, result){
                                        if(result.indexOf(hashtagnode)!=-1){
                                            console.log("TWEET "+ tweet.data.id_str+" CONTAINS " + hashtagnode.data.text);
                                            cb();
                                        } else {
                                            console.log("TWEET "+ tweet.data.id_str+" DOESN'T CONTAIN " + hashtagnode.data.text);
                                            tweet.createRelationshipTo(hashtagnode, "has_hashtag", function(err, rel){
                                                if(err) cb(err);
                                                console.log("CREATED REL: "+ tweet.data.id_str+" "+ rel.type +" "+ hashtagnode.data.text);
                                                cb();
                                            });
                                        }
                                    });
                                } else {
                                    console.log("HASHTAG DOESN'T EXISTS ", ht.text);
                                    var hashtag = neodb.createNode({text: ht.text});
                                    hashtag.save(function (err, node) {
                                        if (err) cb(err);
                                        else {
                                            console.log("CREATED HASHTAG "+ node.data.text);
                                            node.index("Hashtag", "text", ht.text);
                                            tweet.createRelationshipTo(node, "has_hashtag", function(err, rel){
                                                if(err) cb(err);
                                                console.log("CREATED REL: "+ tweet.data.id_str+" "+ rel.type +" "+ node.data.text);
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
                    }
                    else callback(null, user, tweet);
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
                                                replytouser.index("User", "screen_name", entry.in_reply_to_screen_name);
                                                console.log(" CREATED USER: ", replytouser.data.screen_name);
                                                var replytweet = neodb.createNode({id_str: entry.in_reply_to_status_id_str});
                                                replytweet.save(function (err, replytweet) {
                                                    if (err) {
                                                        console.log(err);
                                                        callback(err);
                                                    } else {
                                                        replytweet.index("Tweet", "id_str", entry.in_reply_to_status_id_str);
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

