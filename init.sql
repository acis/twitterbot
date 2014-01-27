CREATE database IF NOT EXISTS users;
USE users;

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id_str` varchar(20) NOT NULL DEFAULT '',
  `screen_name` varchar(100) DEFAULT NULL,
  `name` varchar(300) DEFAULT NULL,
  `description` varchar(160) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `location` varchar(150) DEFAULT NULL,
  `profile_image_url` varchar(300) DEFAULT NULL,
  `profile_image_url_https` varchar(300) DEFAULT NULL,
  `profile_text_color` varchar(6) DEFAULT NULL,
  `url` varchar(300) DEFAULT NULL,
  `listed_count` bigint(20) DEFAULT NULL,
  `favourites_count` bigint(20) DEFAULT NULL,
  `followers_count` bigint(20) DEFAULT NULL,
  `statuses_count` bigint(20) DEFAULT NULL,
  `friends_count` bigint(20) DEFAULT NULL,
  `since_id` varchar(40) DEFAULT NULL,
  `friends` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`id_str`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


