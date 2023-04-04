CREATE TABLE `consumptions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `date` datetime DEFAULT NULL,
  `active` double DEFAULT NULL,
  `inductive` double DEFAULT NULL,
  `capacitive` double DEFAULT NULL,
  `hno` bigint DEFAULT NULL,
  `ssno` bigint DEFAULT NULL,
  `facility_id` int DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `userId` (`facility_id`)
) 