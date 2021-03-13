CREATE TABLE `reddit-submissions` (
  `id` bigint unsigned NOT NULL,
  `title` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `body` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `url` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `epoch` int unsigned NOT NULL,
  `score` int NOT NULL,
  `subreddit` varchar(21) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `author` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `status` smallint unsigned NOT NULL,
  `shortlink` varchar(10) NOT NULL,
  `tags` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `SUBREDDIT_IDX` (`subreddit`),
  KEY `AUTHOR_IDX` (`author`),
  KEY `AUTHOR_BY_SUBREDDIT_IDX` (`author`,`subreddit`)
) ENGINE=TokuDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci