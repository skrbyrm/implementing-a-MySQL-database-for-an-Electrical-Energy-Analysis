# Implementing Consumptions database to MySQL
This script is implementing a MySQL database for an Electrical Energy Analysis web application built with React and Node.js.

Overall, this script sets up a MySQL database for an energy analysis web application, with tables for storing raw consumption data and aggregated monthly, weekly and hourly consumption data. 
It also includes stored procedures for calculating the monthly, weekly and hourly consumption data and aggregating it for all meter IDs.

### consumptions Table
This table stores the consumption data of a facility in a particular time period. Each row represents a record of consumption data, including the date, active power, inductive power, capacitive power, 
and the IDs of the facility and the user who input the data.

```
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
```

### data_by_months Table
This table is used to summarize the consumption data by month for each facility. It includes information such as the district and facility name, 
consumption data (active, inductive, and capacitive power), consumption rate (inductive_ratio and capacitive_ratio), and whether the facility is penalized for overconsumption.
This table is used to summarize the consumption data by month for each facility. It includes information such as the district and facility name, 
consumption data (active, inductive, and capacitive power), consumption rate (inductive_ratio and capacitive_ratio), and whether the facility is penalized for overconsumption.

```
CREATE TABLE `data_by_months` (
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
  `inductive_ratio` double DEFAULT NULL,
  `capacitive_ratio` double DEFAULT NULL,
  `penalized` tinyint(1) DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `userId` (`userId`),
  CONSTRAINT `data_by_months_ibfk_1` FOREIGN KEY (`userId`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) 
```

### Stored Procedure
The monthly_current_by_ssno stored procedure takes in a parameter meterid, which is the ID of the meter that is being analyzed. 
It calculates the consumption data and ratios for the given meter and stores the result in a temporary table temp_month.

The data_by_months stored procedure populates the data_by_months table by looping through each row in the firm_list table and calling the monthly_current_by_ssno stored procedure for each meter.

```
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `monthly_current_by_ssno`(
meterid int
)
BEGIN
DROP TEMPORARY TABLE IF EXISTS temp_month;
CREATE TEMPORARY TABLE temp_month AS
SELECT *
FROM (
	SELECT *, (tab.inductive_ratio >= 20 or tab.capacitive_ratio >= 15 ) penalized
FROM (
	SELECT 
	firm_list.facility, firm_list.district, q.date, q.active, q.capacitive, q.inductive, q.ssno, q.userId
	,ROUND(q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0),2) AS active_cons
	,ROUND(q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0),2) AS inductive_cons
	,ROUND(q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0),2) AS capacitive_cons
	,CASE 
	  WHEN q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0) = 0 THEN 0 
	  ELSE ROUND(((q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0)) / (q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc)))) * 100,4) 
	END AS inductive_ratio
	,CASE 
	  WHEN q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0) = 0 THEN 0 
	  ELSE ROUND(((q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0)) / (q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc)))) * 100,4) 
	END AS capacitive_ratio

	FROM (
	SELECT 
		firm_list.userId userId,
		firm_list.ssno ssno
		,MAX(c.date) date
		,MAX(c.active) active
		,MAX(c.inductive) inductive
		,MAX(c.capacitive) capacitive

	FROM            
		consumptions c
	INNER JOIN
		firm_list ON c.ssno = firm_list.ssno
	GROUP BY firm_list.ssno, firm_list.userId, month(`date`)
	) AS q

	INNER JOIN
			firm_list ON q.ssno = firm_list.ssno
	WHERE firm_list.ssno = meterid
		order by date desc
		limit 1
) AS tab
) AS result
order by result.date DESC;
END ;;
DELIMITER ;
```

#### PROCEDURE `data_by_months`

```
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `data_by_months`(
)
BEGIN
	DECLARE num INT DEFAULT 0;
    TRUNCATE TABLE data_by_months;
	DROP TEMPORARY TABLE IF EXISTS temp_t;
	CREATE TEMPORARY TABLE temp_t
	SELECT ssno,userId FROM firm_list;
	SET @counter = (select count(ssno) from temp_t);

	WHILE num <= @counter DO
		set @assno = (select ssno from temp_t LIMIT 1);
		CALL monthly_current_by_ssno(@assno);
		DELETE FROM temp_t WHERE temp_t.ssno = @assno;
		INSERT INTO data_by_months (facility, district, date, active, capacitive, 
        inductive, ssno, userId, active_cons, inductive_cons, capacitive_cons, 
        inductive_ratio, capacitive_ratio, penalized) SELECT * FROM temp_month;
		SET num = num + 1;
	END WHILE;
DROP TEMPORARY TABLE IF EXISTS temp_t;

END ;;
DELIMITER ;
```

This MySQL stored procedure called update_all that disables foreign key checks, calls four other stored procedures 
(data_by_months, data_by_weeks, data_by_dates, and data_by_hours), and then re-enables foreign key checks.

```
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `update_all`()
BEGIN
SET FOREIGN_KEY_CHECKS = 0;
   CALL data_by_months;
SET FOREIGN_KEY_CHECKS = 1;
   
SET FOREIGN_KEY_CHECKS = 0;
   CALL data_by_weeks;
SET FOREIGN_KEY_CHECKS = 1;
   
SET FOREIGN_KEY_CHECKS = 0;
   CALL data_by_dates;
SET FOREIGN_KEY_CHECKS = 1;   
   
SET FOREIGN_KEY_CHECKS = 0;   
   CALL data_by_hours;
SET FOREIGN_KEY_CHECKS = 1;   
END ;;
DELIMITER ;
```

This code block creates a MySQL event called updatedata that schedules the execution of the update_all stored procedure to run every day at the same time. 
Events are similar to cron jobs in Unix systems and are used to schedule the execution of SQL statements at specific times or intervals.

```
CREATE DEFINER=`scrap`@`%` 
EVENT `updatedata` 
ON SCHEDULE EVERY 1 DAY STARTS '2023-01-07 05:11:06' 
ON COMPLETION NOT PRESERVE ENABLE DO CALL `cons`.`update_all`()
```
