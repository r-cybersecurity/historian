CREATE TABLE `reddit-comments` (
  `id` bigint unsigned NOT NULL,
  `body` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `epoch` int unsigned NOT NULL,
  `score` int NOT NULL,
  `subreddit` varchar(19) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `author` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `status` smallint unsigned NOT NULL,
  `shortlink` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `tags` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `SUBREDDIT_IDX` (`subreddit`) USING BTREE,
  KEY `AUTHOR_IDX` (`author`) USING BTREE,
  KEY `AUTHOR_BY_SUBREDDIT_IDX` (`author`,`subreddit`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci