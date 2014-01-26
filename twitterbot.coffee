request = require("request")
JSONStream = require("JSONStream")
OAuth = require("oauth")
neo4j = require("neo4j")
async = require("async")
mysql = require("mysql")
datejs = require("datejs")
keys = require("./keys.js")
neodb = new neo4j.GraphDatabase("http://localhost:7474")

userArray = ["petecashmore", "denverfoodguy", "BrianBrownNet","GuyKawasaki", "om", "BarackObama","NBA","jack","guardiantech", "stephenfry", "WSJ", "umairh", "rustyrockets", "tinchystryder", "HilaryAlexander", "Zee", "jemimakiss", "RichardDawkins"]
req_count = 0
bearerToken = undefined
mysqlConn = undefined

updateUserInfoQuery = "UPDATE users SET name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https=?, url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?"
userFields = ["name", "description", "created_at", "location", "profile_image_url", "profile_image_url_https", "url", "listed_count", "favourites_count", "followers_count", "statuses_count", "friends_count"]
updateSinceIdQuery = "Update users set since_id=? where id_str = ?"



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
		mysqlConn.query "SELECT screen_name FROM users order by created_at;", (err, result) ->
			console.log err  if err
			result.forEach (user) ->
				userArray.push user.screen_name
			getData userArray
		getFriends()


getFriends = ()->
	async.forever ((next) ->
		setTimeout(()->
			mysqlConn.query "SELECT screen_name FROM users WHERE created_at IS NOT NULL AND friends IS NULL limit 1;", (err, result) ->
				console.log err  if err
				stream = JSONStream.parse()
				options =
					url: "https://api.twitter.com/1.1/friends/list.json?"
					qs: { screen_name: result[0].screen_name, skip_status: true, include_user_entities: false}
					headers:
						"User-Agent": "AIC Data Mining"
						Authorization: "Bearer " + bearerToken

				request(options).pipe stream
				stream.on "root", (obj) ->
					if obj.users?
						async.eachSeries obj.users, ((entry, entrycallback) ->
							console.log "--------------------------------"+result[0].screen_name+" IS FOLLOWING "+ entry.screen_name
							properties =
								id_str: entry.id_str
								name: entry.name
								followers_count: entry.followers_count
								friends_count: entry.friends_count
								listed_count: entry.listed_count
								favourites_count: entry.favourites_count
								statuses_count: entry.statuses_count
							mergeNode "User", "screen_name", entry.screen_name, properties, (err, user) ->
								console.log err if err
								from = { type : 'User',	idKey: 'screen_name',	idVal: result[0].screen_name }
								to = { type : 'User', idKey: 'screen_name',	idVal: entry.screen_name }
								createRelationship from, to, "follows", (err)->
									console.log err if err
									mysqlConn.query "Update users set friends=1 WHERE screen_name=?", result[0].screen_name, (err, result) ->
									if userArray.indexOf(entry.screen_name) is -1
										insertNewUser entry.id_str, entry.screen_name, (err, result)->
											console.log err if err
											userArray.push entry.screen_name
											console.log "------------NEW USER "+entry.screen_name
											entrycallback(err)
									else entrycallback(err)
						), (err) ->
							console.log "ERROR: " + err  if err
							setImmediate next
		, 30000)
	), (err) ->
		console.log err



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

		getTweets
			screen_name: user
			count: 200, ->
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

	#request options, (err, response, body)->
	#	if response.status_code


	request(options).pipe stream
	stream.on "root", (obj) ->
		values = []
		userFields.forEach (field) ->
			if field is "created_at"
				values.push new Date(Date.parse(obj[field]))
			else
				values.push obj[field]

		values.push obj.id_str
		mysqlConn.query updateUserInfoQuery, values, (err, result) ->
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

	stream.on "root", (obj, count) ->

	
		async.eachSeries obj, ((entry, entrycallback) ->
			#console.log entry
			async.waterfall [
				(callback) ->
					properties =
						id_str: entry.user.id_str
						name: entry.user.name
						followers_count: entry.user.followers_count
						friends_count: entry.user.friends_count
						listed_count: entry.user.listed_count
						favourites_count: entry.user.favourites_count
						statuses_count: entry.user.statuses_count

					mergeNode "User", "screen_name", entry.user.screen_name, properties, (err, user) ->
						callback err, entry.user.screen_name

				, (userScreenName, callback) ->
						properties = {
							text: entry.text
							retweet_count: entry.retweet_count
							favorite_count: entry.favorite_count
						}
						mergeNode "Tweet", "id_str", entry.id_str, properties, (err, tweet) ->
							console.log entry.id_str + " " + entry.text+ "..."
							from = { type : 'User',	idKey: 'screen_name',	idVal: userScreenName }
							to = { type : 'Tweet', idKey: 'id_str',	idVal: entry.id_str }
							createRelationship from, to, "tweets", (err) ->
								callback err, userScreenName, entry.id_str

				, (userScreenName, tweetIdStr, callback) ->
						if entry.entities.hashtags.length > 0
							async.forEach entry.entities.hashtags, ((ht, cb) ->
								mergeNode "Hashtag", "text", ht.text, null, (err, hashtag) ->
										console.log err.message + "164543" if err
										from = {type: 'Tweet', idKey: 'id_str', idVal:tweetIdStr }
										to = {type: 'Hashtag', idKey: 'text', idVal: ht.text }
										createRelationship from, to, "has_hashtag", (err) ->
											cb err
								), (err) ->
									console.log err.message + "434321" if err
						
						callback null, userScreenName, tweetIdStr

				, (userScreenName, tweetIdStr, callback) ->
						if entry.in_reply_to_status_id
							mergeNode "User", "screen_name", entry.in_reply_to_screen_name, {id_str: entry.in_reply_to_user_id_str}, (err, replyuser) ->
								console.log  err.message+"342345" if err
								mergeNode "Tweet", "id_str", entry.in_reply_to_status_id_str, null, (err, replytweet) ->
									console.log err.message +"754656"  if err
									query = "MATCH (tweet:Tweet { id_str:'"+tweetIdStr+"' }), (newuser:User { screen_name:'"+entry.in_reply_to_screen_name+"' }), (newtweet:Tweet { id_str:'"+entry.in_reply_to_user_id_str+"' })  "
									query += "MERGE (tweet)-[r:in_reply_to]->(newtweet:Tweet { id_str:'"+entry.in_reply_to_user_id_str+"' })<-[s:tweets]-(newuser:User { screen_name:'"+entry.in_reply_to_screen_name+"' })	RETURN r, s "
									neodb.query query, (err, saved)->
										console.log err.message+"756334" if err
										
						callback null, userScreenName, tweetIdStr

				, (userScreenName, tweetIdStr, callback) ->
						if entry.entities.user_mentions.length > 0
							async.eachSeries entry.entities.user_mentions, ((um, cb) ->
								properties={ id_str: um.id_str, name: um.name,	screen_name: um.screen_name }
								mergeNode 'User', 'screen_name', um.screen_name, properties, (err, mentioned) ->
									###if userArray.indexOf(um.screen_name) is -1 && um.screen_name isnt userScreenName
										insertNewUser(um.id_str, um.screen_name, (e, result) ->
											userArray.push um.screen_name
											console.log "NEW USER ADDED "+ um.screen_name
										)
									###
									from = {type: 'Tweet', idKey:'id_str', idVal:tweetIdStr  }
									to = {type:'User', idKey:'screen_name', idVal:um.screen_name}
									createRelationship from, to, "mentions", (err) ->
										console.log err.message + "756534" if err
										cb
							), (err) ->
								console.log err.message + "48632" if err
						
						callback null, userScreenName, tweetIdStr

			], (result, err) ->
				entrycallback()
		), (err) ->
			console.log "ERROR: " + err  if err
			reqcallback()



insertNewUser = (id_str, screen_name, callback)->
	console.log "params "+id_str+" "+ screen_name
	sql = mysqlConn.query 'INSERT INTO users (id_str, screen_name) values (?, ?)', [id_str, screen_name] , (err, result) ->
		console.log err.message+"95634" if err
		callback err, result


mergeNode = (label, idKey, idVal, properties, callback) ->
	query = "MERGE (node:"+label+" {"+idKey+":'"+idVal+"'}) "
	if properties?
		query += " ON CREATE SET "
		for k,v of properties
			if typeof v == 'number'
				query += " node."+k+"="+v+","
			else
				query += " node."+k+"='"+v+"',"
		query= query.slice(0, -1)
		query += " ON MATCH SET "
		for k,v of properties
			if typeof v == 'number'
				query += " node."+k+"="+v+","
			else
				query += " node."+k+"='"+v+"',"
		query = query.slice(0, -1)
	query += " RETURN node"
	neodb.query query, (err, saved) ->
		callback err, saved


createRelationship = (from, to, type, callback) ->
	query = "MATCH (from:"+from.type+" { "+from.idKey+":'"+from.idVal+"' }),(to:"+to.type+" { "+to.idKey+":'"+to.idVal+"' }) "
	query += "MERGE (from)-[r:"+type+"]->(to)	RETURN r"
	neodb.query query, (err, saved)->
		callback null, saved


init()