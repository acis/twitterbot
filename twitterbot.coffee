request = require('request')
JSONStream = require('JSONStream')
OAuth = require('oauth')
neo4j= require('neo4j')
async = require('async')
mysql = require('mysql')
datejs = require('datejs')
keys = require('./keys.js')



@userArray = ['SarahBrownUK', 'denverfoodguy', 'BrianBrownNet', 'RichardPBacon', 'eddieizzard', 'stephenfry', 'umairh', 'rustyrockets', 'tinchystryder', 'HilaryAlexander', 'Zee', 'jemimakiss', 'RichardDawkins' ]
@neodb
@mysqlConn
@bearerToken
sql = "UPDATE users SET name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https=?, url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?"
userFields = ["name","description", "created_at", "location","profile_image_url","profile_image_url_https","url","listed_count","favourites_count", "followers_count", "statuses_count","friends_count"]



init() ->

	@neodb = new neo4j.GraphDatabase 'http://localhost:7474'

	@mysqlConn = mysql.createConnection
		host     : keys.mysql_host
		database : keys.mysql_db
			user     : keys.mysql_username
			password : keys.mysql_pwd

		@mysqlConn.connect()

	OAuth2 = OAuth.OAuth2
	oauth2 = new OAuth2(keys.consumer_key, keys.consumer_secret, 'https://api.twitter.com/', null, 'oauth2/token', null)
	oauth2.getOAuthAccessToken('', {'grant_type': 'client_credentials'}, (e, access_token, refresh_token, results)->
		@bearerToken = access_token
		#userArray =[];
		@mysqlConn.query('SELECT screen_name FROM users order by created_at asc;', (err, result)->
			console.log err if err

			@userArray.push user.screen_name for user in result

			getData @userArray



getData(userArray) ->

	async.forever((next)->
		user = userArray.shift()
		console.log user
		@mysqlConn.query('SELECT created_at FROM users WHERE screen_name=?', user, (err, result)->
			console.log err if err
			if result[0].created_at is null
				getUserInfo {screen_name:user, include_entities:false}, (err)->
					console.log err if err

		getTweets {screen_name:user, count:200}, ()->
			userArray.push user
			setImmediate next

	, (err)->
			console.log err


getUserInfo(params, callback) ->
	stream = JSONStream.parse()
	options =
		url: "https://api.twitter.com/1.1/users/show.json?"
		qs: params
		headers:
			'User-Agent': 'AIC Data Mining'
			Authorization: "Bearer " + bearerToken

	request(options).pipe stream
	stream.on('root', (obj) ->
		values = []

		for field in @userFields
			if field is "created_at"
				values.push new Date(Date.parse(obj[field]))
			else values.push obj[field]


		values.push obj.id_str

		@mysqlConn.query(sql, values, (err, result)->
			console.log err if err


getTweets(params, reqcallback) ->
	stream = JSONStream.parse()
	options =
		url: "https://api.twitter.com/1.1/statuses/user_timeline.json?"
		qs: params
		headers:
			'User-Agent': 'AIC Data Mining'
			Authorization: "Bearer " + bearerToken

	request(options).pipe stream

	stream.on 'root', (obj) ->
		async.eachSeries obj, (entry, entrycallback) ->
			async.waterfall [
				(callback) ->
					node = @neodb.createNode
						id_str: entry.user.id_str
						name: entry.user.name
						screen_name: entry.user.screen_name
					insertOrUpdate node, "User", "screen_name", entry.user.screen_name, (err, user)->
						callback(err, user)

			, (user, callback) ->
					node = @neodb.createNode
						id_str: entry.id_str
						text: entry.text
						retweet_count: entry.retweet_count
						favorite_count: entry.favorite_count
					insertOrUpdate node, "Tweet", "id_str", entry.id_str, (err, tweet) ->
						createRelationship user, tweet, "tweets", (err)->
							callback err, user, tweet

			, (user, tweet, callback) ->
					if entry.entities.hashtags.length > 0
						async.forEach entry.entities.hashtags, (ht, cb) ->
							node = @neodb.createNode text: ht.text
							insertOrUpdate node, "Hashtag", "text", ht.text, (err, hashtag) ->
								createRelationship tweet, hashtag, "has_hashtag", (err) ->
									cb(err)
						, (err) ->
							callback err, user, tweet
					else
						callback null, user, tweet

			, (user, tweet, callback) ->
					if entry.in_reply_to_status_id
						node = @neodb.createNode
							id_str: entry.in_reply_to_user_id_str
							screen_name: entry.in_reply_to_screen_name
						insertOrUpdate node, "User", "screen_name", entry.in_reply_to_screen_name, (err, replyuser) ->
							callback err if err
							node = @neodb.createNode(id_str: entry.in_reply_to_status_id_str)
							insertOrUpdate node, "Tweet", "id_str", entry.in_reply_to_status_id_str, (err, replytweet) ->
								createRelationship tweet, replytweet, "in_reply_to", (err) ->
									callback err, user, tweet

					else
						callback null, user, tweet

			], (result, err) ->
				entrycallback()

		, (err)->
			console.log "ERROR: " + err if err
			reqcallback()














init()


