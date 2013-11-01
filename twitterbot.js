var https = require('https');
var	qs = require('qs');
var	request = require('request');
var	JSONStream = require('JSONStream');
var	OAuth = require('oauth');
var	neo4j= require('neo4j');
var	streamline = require('streamline');
var	es = require('event-stream');
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
	var params={screen_name:'twitterapi'};
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
		obj.forEach(function(entry) {
			//SAVE DATA
		/*	var node = db.createNode({hello: 'world'});
			node.save(function (err, node) {
				if (err) {
					console.err('Error saving new node to database:', err);
				} else {
					console.log('Node saved to database with id:', node.id);
				}
			}); */
		});





	})

}

getBearerToken();

