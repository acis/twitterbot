(function() {
  var JSONStream, OAuth, async, bearerToken, createRelationship, datejs, getData, getFriends, getTweets, getUserInfo, init, insertNewUser, keys, mergeNode, mysql, mysqlConn, neo4j, neodb, req_count, request, updateSinceIdQuery, updateUserInfoQuery, userArray, userFields;

  request = require("request");

  JSONStream = require("JSONStream");

  OAuth = require("oauth");

  neo4j = require("neo4j");

  async = require("async");

  mysql = require("mysql");

  datejs = require("datejs");

  keys = require("./keys.js");

  neodb = new neo4j.GraphDatabase("http://localhost:7474");

  userArray = ["petecashmore", "denverfoodguy", "BrianBrownNet", "GuyKawasaki", "om", "BarackObama", "NBA", "jack", "guardiantech", "stephenfry", "WSJ", "umairh", "rustyrockets", "tinchystryder", "HilaryAlexander", "Zee", "jemimakiss", "RichardDawkins"];

  req_count = 0;

  bearerToken = void 0;

  mysqlConn = void 0;

  updateUserInfoQuery = "UPDATE users SET name = ?, description = ?, created_at = ?, location=?, profile_image_url=?, profile_image_url_https=?, url=?, listed_count=?, favourites_count=?, followers_count=?, statuses_count= ?,friends_count = ? where id_str= ?";

  userFields = ["name", "description", "created_at", "location", "profile_image_url", "profile_image_url_https", "url", "listed_count", "favourites_count", "followers_count", "statuses_count", "friends_count"];

  updateSinceIdQuery = "Update users set since_id=? where id_str = ?";

  init = function() {
    var OAuth2, oauth2;
    neodb = new neo4j.GraphDatabase("http://localhost:7474");
    mysqlConn = mysql.createConnection({
      host: keys.mysql_host,
      database: keys.mysql_db,
      user: keys.mysql_username,
      password: keys.mysql_pwd
    });
    mysqlConn.connect();
    OAuth2 = OAuth.OAuth2;
    oauth2 = new OAuth2(keys.consumer_key, keys.consumer_secret, "https://api.twitter.com/", null, "oauth2/token", null);
    return oauth2.getOAuthAccessToken("", {
      grant_type: "client_credentials"
    }, function(e, access_token, refresh_token, results) {
      bearerToken = access_token;
      userArray = [];
      mysqlConn.query("SELECT screen_name FROM users order by created_at;", function(err, result) {
        if (err) {
          console.log(err);
        }
        result.forEach(function(user) {
          return userArray.push(user.screen_name);
        });
        return getData(userArray);
      });
      return getFriends();
    });
  };

  getFriends = function() {
    return async.forever((function(next) {
      return setTimeout(function() {
        return mysqlConn.query("SELECT screen_name FROM users WHERE created_at IS NOT NULL AND friends IS NULL limit 1;", function(err, result) {
          var options, stream;
          if (err) {
            console.log(err);
          }
          stream = JSONStream.parse();
          options = {
            url: "https://api.twitter.com/1.1/friends/list.json?",
            qs: {
              screen_name: result[0].screen_name,
              skip_status: true,
              include_user_entities: false
            },
            headers: {
              "User-Agent": "AIC Data Mining",
              Authorization: "Bearer " + bearerToken
            }
          };
          request(options).pipe(stream);
          return stream.on("root", function(obj) {
            if (obj.users != null) {
              return async.eachSeries(obj.users, (function(entry, entrycallback) {
                var properties;
                console.log("--------------------------------" + result[0].screen_name + " IS FOLLOWING " + entry.screen_name);
                properties = {
                  id_str: entry.id_str,
                  name: entry.name,
                  followers_count: entry.followers_count,
                  friends_count: entry.friends_count,
                  listed_count: entry.listed_count,
                  favourites_count: entry.favourites_count,
                  statuses_count: entry.statuses_count
                };
                return mergeNode("User", "screen_name", entry.screen_name, properties, function(err, user) {
                  var from, to;
                  if (err) {
                    console.log(err);
                  }
                  from = {
                    type: 'User',
                    idKey: 'screen_name',
                    idVal: result[0].screen_name
                  };
                  to = {
                    type: 'User',
                    idKey: 'screen_name',
                    idVal: entry.screen_name
                  };
                  return createRelationship(from, to, "follows", function(err) {
                    if (err) {
                      console.log(err);
                    }
                    mysqlConn.query("Update users set friends=1 WHERE screen_name=?", result[0].screen_name, function(err, result) {});
                    if (userArray.indexOf(entry.screen_name) === -1) {
                      return insertNewUser(entry.id_str, entry.screen_name, function(err, result) {
                        if (err) {
                          console.log(err);
                        }
                        userArray.push(entry.screen_name);
                        console.log("------------NEW USER " + entry.screen_name);
                        return entrycallback(err);
                      });
                    } else {
                      return entrycallback(err);
                    }
                  });
                });
              }), function(err) {
                if (err) {
                  console.log("ERROR: " + err);
                }
                return setImmediate(next);
              });
            }
          });
        });
      }, 30000);
    }), function(err) {
      return console.log(err);
    });
  };

  getData = function(userArray) {
    return async.forever((function(next) {
      var user;
      user = userArray.shift();
      console.log(user);
      mysqlConn.query("SELECT created_at FROM users WHERE screen_name=?", user, function(err, result) {
        if (err) {
          console.log(err);
        }
        if (result[0].created_at == null) {
          return getUserInfo({
            screen_name: user,
            include_entities: false
          }, function(err) {
            if (err) {
              return console.log(err);
            }
          });
        }
      });
      return getTweets({
        screen_name: user,
        count: 200
      }, function() {
        userArray.push(user);
        return setImmediate(next);
      });
    }), function(err) {
      return console.log(err);
    });
  };

  getUserInfo = function(params, callback) {
    var options, stream;
    stream = JSONStream.parse();
    options = {
      url: "https://api.twitter.com/1.1/users/show.json?",
      qs: params,
      headers: {
        "User-Agent": "AIC Data Mining",
        Authorization: "Bearer " + bearerToken
      }
    };
    request(options).pipe(stream);
    return stream.on("root", function(obj) {
      var values;
      values = [];
      userFields.forEach(function(field) {
        if (field === "created_at") {
          return values.push(new Date(Date.parse(obj[field])));
        } else {
          return values.push(obj[field]);
        }
      });
      values.push(obj.id_str);
      return mysqlConn.query(updateUserInfoQuery, values, function(err, result) {
        if (err) {
          return console.log(err);
        }
      });
    });
  };

  getTweets = function(params, reqcallback) {
    var options, stream;
    stream = JSONStream.parse();
    options = {
      url: "https://api.twitter.com/1.1/statuses/user_timeline.json?",
      qs: params,
      headers: {
        "User-Agent": "AIC Data Mining",
        Authorization: "Bearer " + bearerToken
      }
    };
    request(options).pipe(stream);
    return stream.on("root", function(obj, count) {
      return async.eachSeries(obj, (function(entry, entrycallback) {
        return async.waterfall([
          function(callback) {
            var properties;
            properties = {
              id_str: entry.user.id_str,
              name: entry.user.name,
              followers_count: entry.user.followers_count,
              friends_count: entry.user.friends_count,
              listed_count: entry.user.listed_count,
              favourites_count: entry.user.favourites_count,
              statuses_count: entry.user.statuses_count
            };
            return mergeNode("User", "screen_name", entry.user.screen_name, properties, function(err, user) {
              return callback(err, entry.user.screen_name);
            });
          }, function(userScreenName, callback) {
            var properties;
            properties = {
              text: entry.text,
              retweet_count: entry.retweet_count,
              favorite_count: entry.favorite_count
            };
            return mergeNode("Tweet", "id_str", entry.id_str, properties, function(err, tweet) {
              var from, to;
              console.log(entry.id_str + " " + entry.text + "...");
              from = {
                type: 'User',
                idKey: 'screen_name',
                idVal: userScreenName
              };
              to = {
                type: 'Tweet',
                idKey: 'id_str',
                idVal: entry.id_str
              };
              return createRelationship(from, to, "tweets", function(err) {
                return callback(err, userScreenName, entry.id_str);
              });
            });
          }, function(userScreenName, tweetIdStr, callback) {
            if (entry.entities.hashtags.length > 0) {
              async.forEach(entry.entities.hashtags, (function(ht, cb) {
                return mergeNode("Hashtag", "text", ht.text, null, function(err, hashtag) {
                  var from, to;
                  if (err) {
                    console.log(err.message + "164543");
                  }
                  from = {
                    type: 'Tweet',
                    idKey: 'id_str',
                    idVal: tweetIdStr
                  };
                  to = {
                    type: 'Hashtag',
                    idKey: 'text',
                    idVal: ht.text
                  };
                  return createRelationship(from, to, "has_hashtag", function(err) {
                    return cb(err);
                  });
                });
              }), function(err) {
                if (err) {
                  return console.log(err.message + "434321");
                }
              });
            }
            return callback(null, userScreenName, tweetIdStr);
          }, function(userScreenName, tweetIdStr, callback) {
            if (entry.in_reply_to_status_id) {
              mergeNode("User", "screen_name", entry.in_reply_to_screen_name, {
                id_str: entry.in_reply_to_user_id_str
              }, function(err, replyuser) {
                if (err) {
                  console.log(err.message + "342345");
                }
                return mergeNode("Tweet", "id_str", entry.in_reply_to_status_id_str, null, function(err, replytweet) {
                  var query;
                  if (err) {
                    console.log(err.message(+"754656"));
                  }
                  query = "MATCH (tweet:Tweet { id_str:'" + tweetIdStr + "' }), (newuser:User { screen_name:'" + entry.in_reply_to_screen_name + "' }), (newtweet:Tweet { id_str:'" + entry.in_reply_to_user_id_str + "' })  ";
                  query += "MERGE (tweet)-[r:in_reply_to]->(newtweet:Tweet { id_str:'" + entry.in_reply_to_user_id_str + "' })<-[s:tweets]-(newuser:User { screen_name:'" + entry.in_reply_to_screen_name + "' })	RETURN r, s ";
                  return neodb.query(query, function(err, saved) {
                    if (err) {
                      return console.log(err.message + "756334");
                    }
                  });
                });
              });
            }
            return callback(null, userScreenName, tweetIdStr);
          }, function(userScreenName, tweetIdStr, callback) {
            if (entry.entities.user_mentions.length > 0) {
              async.eachSeries(entry.entities.user_mentions, (function(um, cb) {
                var properties;
                properties = {
                  id_str: um.id_str,
                  name: um.name,
                  screen_name: um.screen_name
                };
                return mergeNode('User', 'screen_name', um.screen_name, properties, function(err, mentioned) {
                  /*if userArray.indexOf(um.screen_name) is -1 && um.screen_name isnt userScreenName
                  										insertNewUser(um.id_str, um.screen_name, (e, result) ->
                  											userArray.push um.screen_name
                  											console.log "NEW USER ADDED "+ um.screen_name
                  										)
                  */

                  var from, to;
                  from = {
                    type: 'Tweet',
                    idKey: 'id_str',
                    idVal: tweetIdStr
                  };
                  to = {
                    type: 'User',
                    idKey: 'screen_name',
                    idVal: um.screen_name
                  };
                  return createRelationship(from, to, "mentions", function(err) {
                    if (err) {
                      console.log(err.message + "756534");
                    }
                    return cb;
                  });
                });
              }), function(err) {
                if (err) {
                  return console.log(err.message + "48632");
                }
              });
            }
            return callback(null, userScreenName, tweetIdStr);
          }
        ], function(result, err) {
          return entrycallback();
        });
      }), function(err) {
        if (err) {
          console.log("ERROR: " + err);
        }
        return reqcallback();
      });
    });
  };

  insertNewUser = function(id_str, screen_name, callback) {
    var sql;
    console.log("params " + id_str + " " + screen_name);
    return sql = mysqlConn.query('INSERT INTO users (id_str, screen_name) values (?, ?)', [id_str, screen_name], function(err, result) {
      if (err) {
        console.log(err.message + "95634");
      }
      return callback(err, result);
    });
  };

  mergeNode = function(label, idKey, idVal, properties, callback) {
    var k, query, v;
    query = "MERGE (node:" + label + " {" + idKey + ":'" + idVal + "'}) ";
    if (properties != null) {
      query += " ON CREATE SET ";
      for (k in properties) {
        v = properties[k];
        if (typeof v === 'number') {
          query += " node." + k + "=" + v + ",";
        } else {
          query += " node." + k + "='" + v + "',";
        }
      }
      query = query.slice(0, -1);
      query += " ON MATCH SET ";
      for (k in properties) {
        v = properties[k];
        if (typeof v === 'number') {
          query += " node." + k + "=" + v + ",";
        } else {
          query += " node." + k + "='" + v + "',";
        }
      }
      query = query.slice(0, -1);
    }
    query += " RETURN node";
    return neodb.query(query, function(err, saved) {
      return callback(err, saved);
    });
  };

  createRelationship = function(from, to, type, callback) {
    var query;
    query = "MATCH (from:" + from.type + " { " + from.idKey + ":'" + from.idVal + "' }),(to:" + to.type + " { " + to.idKey + ":'" + to.idVal + "' }) ";
    query += "MERGE (from)-[r:" + type + "]->(to)	RETURN r";
    return neodb.query(query, function(err, saved) {
      return callback(null, saved);
    });
  };

  init();

}).call(this);
