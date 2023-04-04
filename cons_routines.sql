DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `daily_by_ssno`(
meterid int
)
BEGIN
DROP TEMPORARY TABLE IF EXISTS temp_day;
CREATE TEMPORARY TABLE temp_day AS
SELECT *
FROM (
	SELECT *
FROM (
	SELECT 
	firm_list.facility, firm_list.district, q.date, q.active, q.capacitive, q.inductive, q.ssno, q.userId
	,ROUND(q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0),2) AS active_cons
	,ROUND(q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0),2) AS inductive_cons
	,ROUND(q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0),2) AS capacitive_cons
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
	GROUP BY firm_list.ssno, firm_list.userId, day(`date`)
	) AS q

	INNER JOIN
			firm_list ON q.ssno = firm_list.ssno
	WHERE firm_list.ssno = meterid
    order by date ASC
    LIMIT 1000 OFFSET 1
) AS tab
) AS result
order by result.date DESC;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `data_by_dates`(
)
BEGIN
	DECLARE num INT DEFAULT 0;
    TRUNCATE TABLE data_by_dates;
	DROP TEMPORARY TABLE IF EXISTS temp_t;
	CREATE TEMPORARY TABLE temp_t
	SELECT ssno,userId FROM firm_list;
	SET @counter = (select count(ssno) from temp_t);
    
	WHILE num <= @counter DO
		set @assno = (select ssno from temp_t LIMIT 1);
		CALL daily_by_ssno(@assno);
		DELETE FROM temp_t WHERE temp_t.ssno = @assno;
		INSERT INTO data_by_dates (facility, district, date, active, capacitive, 
        inductive, ssno, userId, active_cons, inductive_cons, capacitive_cons)  SELECT *FROM temp_day;
		SET num = num + 1;
	END WHILE;
DROP TEMPORARY TABLE IF EXISTS temp_t;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `data_by_hours`(
)
BEGIN
	DECLARE num INT DEFAULT 0;
    TRUNCATE TABLE data_by_hours;
	DROP TEMPORARY TABLE IF EXISTS temp_t;
	CREATE TEMPORARY TABLE temp_t
	SELECT ssno,userId FROM firm_list;
	SET @counter = (select count(ssno) from temp_t);
    
	WHILE num <= @counter DO
		set @assno = (select ssno from temp_t LIMIT 1);
		CALL hourly_by_ssno(@assno);
		DELETE FROM temp_t WHERE temp_t.ssno = @assno;
		INSERT INTO data_by_hours (facility, district, date, active, capacitive, 
        inductive, ssno, userId, active_cons, inductive_cons, capacitive_cons) SELECT *FROM temp_hour;
		SET num = num + 1;
	END WHILE;
DROP TEMPORARY TABLE IF EXISTS temp_t;
END ;;
DELIMITER ;

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

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `data_by_weeks`(
)
BEGIN
	DECLARE num INT DEFAULT 0;
    TRUNCATE TABLE data_by_weeks;
	DROP TEMPORARY TABLE IF EXISTS temp_t;
	CREATE TEMPORARY TABLE temp_t
	SELECT ssno,userId FROM firm_list;
	SET @counter = (select count(ssno) from temp_t);

	WHILE num <= @counter DO
		set @assno = (select ssno from temp_t LIMIT 1);
		CALL weekly(@assno);
		DELETE FROM temp_t WHERE temp_t.ssno = @assno;
		INSERT INTO data_by_weeks (facility, district, date, active, capacitive, 
        inductive, ssno, userId, active_cons, inductive_cons, capacitive_cons, 
        inductive_ratio, capacitive_ratio, penalized) SELECT *FROM temp_week;
		SET num = num + 1;
	END WHILE;
DROP TEMPORARY TABLE IF EXISTS temp_t;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `hourly_by_ssno`(
meterid int
)
BEGIN
DROP TEMPORARY TABLE IF EXISTS temp_hour;
CREATE TEMPORARY TABLE temp_hour AS
SELECT *
FROM (
	SELECT *
FROM (
	SELECT 
	firm_list.facility, firm_list.district, q.date, q.active, q.capacitive, q.inductive, q.ssno, q.userId
	,ROUND(q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0),2) AS active_cons
	,ROUND(q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0),2) AS inductive_cons
	,ROUND(q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0),2) AS capacitive_cons
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
	GROUP BY firm_list.ssno, firm_list.userId, hour(`date`)
	) AS q

	INNER JOIN
			firm_list ON q.ssno = firm_list.ssno
	WHERE firm_list.ssno = meterid
    order by date ASC
    LIMIT 100 OFFSET 1
) AS tab
) AS result
order by result.date DESC;
END ;;
DELIMITER ;

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

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `weekly`(
meterid int
)
BEGIN

DROP TEMPORARY TABLE IF EXISTS temp_week;
CREATE TEMPORARY TABLE temp_week AS
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
	  WHEN q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0) = 0 THEN 1 
	  ELSE ROUND(((q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0)) / (q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc)))) * 100,4) 
	END AS inductive_ratio
	,CASE 
	  WHEN q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0) = 0 THEN 1 
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
	GROUP BY firm_list.ssno, firm_list.userId, week(`date`)
	) AS q

	INNER JOIN
			firm_list ON q.ssno = firm_list.ssno
	WHERE firm_list.ssno = meterid
    order by date ASC
    LIMIT 100 OFFSET 1
) AS tab
) AS result
order by result.date DESC;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `weekly_by_ssno`(
meterid int
)
BEGIN

DROP TEMPORARY TABLE IF EXISTS temp_week;
CREATE TEMPORARY TABLE temp_week AS
SELECT *
FROM (
	SELECT *, (tab.inductive_ratio >= 20 or tab.capacitive_ratio >= 15 ) penalized
FROM (
	SELECT 
	firm_list.facility, firm_list.district, q.date, q.active, q.capacitive, q.inductive, q.ssno, q.userId
	,ROUND(q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc), 0),2) AS active_cons
	,ROUND(q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0),2) AS inductive_cons
	,ROUND(q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0),2) AS capacitive_cons
	,ROUND(((q.inductive - coalesce(lag(q.inductive) over (partition by  q.ssno order by q.date asc), 0)) / (q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc)))) * 100,4) AS inductive_ratio
	,ROUND(((q.capacitive - coalesce(lag(q.capacitive) over (partition by  q.ssno order by q.date asc), 0)) / (q.active - coalesce(lag(q.active) over (partition by  q.ssno order by q.date asc)))) * 100,4) AS capacitive_ratio

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
	GROUP BY firm_list.ssno, firm_list.userId, week(`date`)
	) AS q

	INNER JOIN
			firm_list ON q.ssno = firm_list.ssno
	WHERE firm_list.ssno = meterid
    order by date ASC
    LIMIT 100 OFFSET 1
) AS tab
) AS result
order by result.date DESC;
END ;;
DELIMITER ;

