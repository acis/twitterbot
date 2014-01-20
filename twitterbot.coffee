request = require("request")
JSONStream = require("JSONStream")
OAuth = require("oauth")
neo4j = require("neo4j")
async = require("async")
mysql = require("mysql")
datejs = require("datejs")
keys = require("./keys.js")
neodb = new neo4j.GraphDatabase("http://localhost:7474")

userArray = ["SarahBrownUK", "denverfoodguy", "BrianBrownNet", "RichardPBacon", "eddieizzard", "stephenfry", "umairh", "rustyrockets", "tinchystryder", "HilaryAlexander", "Zee", "jemimakiss", "RichardDawkins"]
req_count = 0
bearerToken = undefined
mysqlConn = undefined
sql = "UPDATE users SET name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https=?, url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?"
userFields = ["name", "description", "created_at", "location", "profile_image_url", "profile_image_url_https", "url", "listed_count", "favourites_count", "followers_count", "statuses_count", "friends_count"]


init = ->
	neodb = new neo4j.GraphDatabase("http://localhost:7474")
	mysqlConn = mysql.createConnection(
		host: keys.mysql_host
		database: keys.mysql_db
		user: keys.mysql_username
		password: keys.mysql_pwd
	)
	mysqlConn.connect()
	OAuth2 = OAuth.OAuth2
	oauth2 = new OAuth2(keys.consumer_key, keys.consumer_secret, "https://api.twitter.com/", null, "oauth2/token", null)
	oauth2.getOAuthAccessToken "",
		grant_type: "client_credentials"
	, (e, access_token, refresh_token, results) ->
		bearerToken = access_token
		userArray = []
		mysqlConn.query "SELECT screen_name FROM users;", (err, result) ->
			console.log err  if err
			result.forEach (user) ->
				userArray.push user.screen_name

			getData userArray



getData = (userArray) ->
	async.forever ((next) ->
		user = userArray.shift()
		console.log user
		mysqlConn.query "SELECT created_at FROM users WHERE screen_name=?", user, (err, result) ->
			console.log err  if err
			unless result[0].created_at?
				getUserInfo
					screen_name: user
					include_entities: false
				, (err) ->
					console.log err  if err


		console.log user
		getTweets
			screen_name: user
			count: 200
		, ->
			userArray.push user
			setImmediate next

	), (err) ->
		console.log err

getUserInfo = (params, callback) ->
	stream = JSONStream.parse()
	options =
		url: "https://api.twitter.com/1.1/users/show.json?"
		qs: params
		headers:
			"User-Agent": "AIC Data Mining"
			Authorization: "Bearer " + bearerToken

	request(options).pipe stream
	stream.on "root", (obj) ->
		values = []
		userFields.forEach (field) ->
			if field is "created_at"
				values.push new Date(Date.parse(obj[field]))
			else
				values.push obj[field]

		values.push obj.id_str
		mysqlConn.query sql, values, (err, result) ->
			console.log err  if err


getTweets = (params, reqcallback) ->
	stream = JSONStream.parse()
	options =
		url: "https://api.twitter.com/1.1/statuses/user_timeline.json?"
		qs: params
		headers:
			"User-Agent": "AIC Data Mining"
			Authorization: "Bearer " + bearerToken

	request(options).pipe stream
	stream.on "root", (obj) ->
		async.eachSeries obj, ((entry, entrycallback) ->

			#console.log("\n\n\nENTRY:",entry);
			console.log "\n\n"
			async.waterfall [
				(callback) ->
					properties = { id_str: entry.user.id_str, name: entry.user.name	}
					mergeNode "User", "screen_name", entry.user.screen_name, properties, (err, user) ->
						callback err, entry.user.screen_name

				, (userScreenName, callback) ->
						properties = {
							text: entry.text
							retweet_count: entry.retweet_count
							favorite_count: entry.favorite_count
						}
						mergeNode "Tweet", "id_str", entry.id_str, properties, (err, tweet) ->
							from = { type : 'User',	idKey: 'screen_name',	idVal: userScreenName }
							to = { type : 'Tweet', idKey: 'id_str',	idVal: entry.id_str }
							createRelationship from, to, "tweets", (err) ->
								callback err, userScreenName, entry.id_str

				, (userScreenName, tweetIdStr, callback) ->
						if entry.entities.hashtags.length > 0
							async.forEach entry.entities.hashtags, ((ht, cb) ->
								mergeNode "Hashtag", "text", ht.text, null, (err, hashtag) ->
									from = {type: 'Tweet', idKey: 'id_str', idVal:tweetIdStr }
									to = {type: 'Hashtag', idKey: 'text', idVal: ht.text }
									createRelationship from, to, "has_hashtag", (err) ->
										cb err
							), (err) ->
								callback err, userScreenName, tweetIdStr
						else
							callback null, userScreenName, tweetIdStr

				, (userScreenName, tweetIdStr, callback) ->
						if entry.in_reply_to_status_id
							mergeNode "User", "screen_name", entry.in_reply_to_screen_name, {id_str: entry.in_reply_to_user_id_str}, (err, replyuser) ->
								callback err  if err
								userArray.push entry.in_reply_to_screen_name  if userArray.indexOf(entry.in_reply_to_screen_name) is -1
								mergeNode "Tweet", "id_str", entry.in_reply_to_status_id_str, null, (err, replytweet) ->
									callback err  if err
									query = "MATCH (tweet:Tweet { id_str:'"+tweetIdStr+"' }), (newtweet:Tweet { id_str:'"+entry.in_reply_to_user_id_str+"' }), (newuser:User { screen_name:'"+entry.in_reply_to_screen_name+"' })  "
									query += "MERGE (tweet)-[r:in_reply_to]->(newtweet)<-[s:tweets]-(newuser)	RETURN r, s "
									#console.log query
									neodb.query query, (err, saved)->
										#console.log err, saved
										#callback null, saved
									callback null, userScreenName, tweetIdStr
						else
							callback null, userScreenName, tweetIdStr

				, (userScreenName, tweetIdStr, callback) ->
						if entry.entities.user_mentions.length > 0
							async.forEach entry.entities.user_mentions, ((um, cb) ->
								properties={ id_str: um.id_str, name: um.name,	screen_name: um.screen_name }
								mergeNode 'User', 'screen_name', um.screen_name, properties, (err, mentioned) ->
									userArray.push um.screen_name  if userArray.indexOf(um.screen_name) is -1
									from = {type: 'Tweet', idKey:'id_str', tweetIdStr }
									to = {type:'User', idKey:'screen_name', idVal:um.screen_name}
									createRelationship from, to, "mentions", (err) ->
										cb err


							), (err) ->
								callback err  if err
								callback null, userScreenName, tweetIdStr

						else
							callback null, userScreenName, tweetIdStr

			], (result, err) ->
				entrycallback()

		), (err) ->
			console.log "ERROR: " + err  if err
			reqcallback()


mergeNode = (label, idKey, idVal, properties, callback) ->
	query = "MERGE (node:"+label+" {"+idKey+":'"+idVal+"'}) "
	query += " ON CREATE SET"
	for k,v of properties
		query += " node."+k+"='"+v+"',"
	query= query.slice(0, -1)
	query += " ON MATCH SET "
	for k,v of properties
		query += " node."+k+"='"+v+"',"
	query = query.slice(0, -1)
	query += " RETURN node"
	console.log query
	#console.log callback
	neodb.query query, (err, saved) ->
		callback null, saved


createRelationship = (from, to, type, callback) ->
	###MATCH (charlie:Person { name:'Charlie Sheen' }),(wallStreet:Movie { title:'Wall Street' })
	MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
	RETURN r
	###
	query = "MATCH (from:"+from.type+" { "+from.idKey+":'"+from.idVal+"' }),(to:"+to.type+" { "+to.idKey+":'"+to.idVal+"' }) "
	query += "MERGE (from)-[r:"+type+"]->(to)	RETURN r"
	console.log query
	neodb.query query, (err, saved)->
		#console.log err, saved
		callback null, saved


init()