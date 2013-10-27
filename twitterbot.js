var https = require('https'),
	OAuth = require('oauth'),
	keys = require('./keys.js');


//get bearer token
var OAuth2 = OAuth.OAuth2;

var oauth2 = new OAuth2(keys.consumer_key,keys.consumer_secret,'https://api.twitter.com/', null, 'oauth2/token', null);

oauth2.getOAuthAccessToken('',{'grant_type':'client_credentials'},function (e, access_token, refresh_token, results){

        makeRequest(access_token);
		}
);


function makeRequest(bearer_token){

    var options = {
        hostname: 'api.twitter.com',
        port:443,
        //path:"/1.1/statuses/user_timeline.json?screen_name=twitterapi&count=2",
        path: '/1.1/search/tweets.json?q=%23freebandnames&since_id=24012619984051000&max_id=250126199840518145&result_type=mixed&count=4',
        method:"GET",
        headers: {
            'User-Agent': 'AIC Data Mining',
            Authorization: "Bearer "+bearer_token
        }
    };

      var req = https.request(options, function(res) {
        res.on('data', function(d) {
            process.stdout.write(d);
        });
    });
    req.end();

    req.on('error', function(e) {
        console.error(e);
    });

}

