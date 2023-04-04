
CREATE TABLE `data_by_hours` (
  `id` int NOT NULL AUTO_INCREMENT,
  `facility` text,
  `district` text,
  `date` datetime DEFAULT NULL,
  `active` double DEFAULT NULL,
  `capacitive` double DEFAULT NULL,
  `inductive` double DEFAULT NULL,
  `ssno` bigint DEFAULT NULL,
  `userId` int DEFAULT NULL,
  `active_cons` double DEFAULT NULL,
  `inductive_cons` double DEFAULT NULL,
  `capacitive_cons` double DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `userId` (`userId`),
  CONSTRAINT `data_by_hours_ibfk_1` FOREIGN KEY (`userId`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
)