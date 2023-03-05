#QUERY 1: Change the format of phone number to (XXX)XXX-XXXX
ALTER TABLE cdw_sapp_custmer 
MODIFY CUST_PHONE VARCHAR(14);

UPDATE cdw_sapp_custmer 
SET CUST_PHONE = CONCAT('(', '516', ')', SUBSTR(CUST_PHONE, 1,3), '-', SUBSTR(CUST_PHONE, 4,7));

#QUERY 2: Concatenate Apartment no and Street name of customer residence with space as a seperator
ALTER TABLE cdw_sapp_custmer ADD FULL_STREET_ADDRESS VARCHAR(50);
UPDATE cdw_sapp_custmer
SET FULL_STREET_ADDRESS= CONCAT(APT_NO, ' , ', STREET_NAME);

#Drop APT_NO AND STREET_NAME columns
ALTER TABLE cdw_sapp_custmer
DROP COLUMN APT_NO, 
DROP COLUMN STREET_NAME;

#QUERY 3: Change the format of phone number to (XXX)XXX-XXXX
ALTER TABLE cdw_sapp_branch
MODIFY BRANCH_PHONE VARCHAR(14);

UPDATE cdw_sapp_branch
SET BRANCH_PHONE = CONCAT('(', SUBSTR(BRANCH_PHONE, 1,3), ')', SUBSTR(BRANCH_PHONE,4,3), '-', SUBSTR(BRANCH_PHONE, 7,4));

#QUERY 4: Set default to (99999) if source value is null
ALTER TABLE cdw_sapp_branch
MODIFY COLUMN BRANCH_ZIP INTEGER DEFAULT 99999;

INSERT INTO cdw_sapp_branch (BRANCH_ZIP)
VALUES (COALESCE(99999));

#QUERY 5: Convert DAY, MONTH, and YEAR into a TIMEID(YYYYMMDD)
#Step 1: convert M to MM
ALTER TABLE cdw_sapp_credit
MODIFY COLUMN MONTH VARCHAR(2);

UPDATE cdw_sapp_credit
SET MONTH = CONCAT('0', MONTH)
WHERE LENGTH(MONTH) = 1;

#Step 2: convert D to DD
ALTER TABLE cdw_sapp_credit
MODIFY COLUMN DAY VARCHAR(2);

UPDATE cdw_sapp_credit
SET DAY = CONCAT('0', DAY)
WHERE LENGTH(DAY) = 1;

#Step 3: combine columns
ALTER TABLE cdw_sapp_credit ADD TIMEID VARCHAR(11);
UPDATE cdw_sapp_credit
SET TIMEID = CONCAT(YEAR,MONTH,DAY);

#Step 4: drop MONTH, DAY, YEAR Columns
ALTER TABLE cdw_sapp_credit
DROP COLUMN MONTH,
DROP COLUMN YEAR,
DROP COLUMN DAY;

#QUERY 6: Display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order
SELECT *
FROM(
	SELECT customer.CUST_ZIP, credit.*
	FROM cdw_sapp_custmer AS customer
	JOIN cdw_sapp_credit AS credit
	ON customer.CREDIT_CARD_NO = credit.CREDIT_CARD_NO
	) AS cust_credit
WHERE cust_credit.CUST_ZIP = '52804'
AND SUBSTRING(cust_credit.TIMEID,5,2) = '08'
AND SUBSTRING(cust_credit.TIMEID,1,4) = '2018'
ORDER BY SUBSTRING(cust_credit.TIMEID,7,2)  DESC

#QUERY 7: Display the number and total values of transactions for a each Transaction type.
SELECT DISTINCT(TRANSACTION_TYPE), COUNT(TRANSACTION_TYPE) AS Transaction_count, ROUND(SUM(TRANSACTION_VALUE),2) AS Transaction_Sum
FROM cdw_sapp_credit
GROUP BY TRANSACTION_TYPE

#QUERY 8: Display the total number and total values of transactions for branches in each state.
SELECT branch_credit.BRANCH_STATE, COUNT(TRANSACTION_TYPE) AS Total_Transactions, ROUND(SUM(TRANSACTION_VALUE),2) AS Total_Values
FROM(
	SELECT branch.BRANCH_STATE, credit.*
	FROM cdw_sapp_branch AS branch
	JOIN cdw_sapp_credit AS credit
	ON branch.BRANCH_CODE = credit.BRANCH_CODE
	) AS branch_credit
GROUP BY branch_credit.BRANCH_STATE

#QUERY 9: Display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
SELECT cust_credit.*
FROM (
	SELECT customer.FIRST_NAME, customer.LAST_NAME, credit.*
	FROM cdw_sapp_custmer AS customer
	JOIN cdw_sapp_credit AS credit
	ON customer.CREDIT_CARD_NO = credit.CREDIT_CARD_NO
	) AS cust_credit
WHERE cust_credit.TIMEID BETWEEN '20180601' AND '20181001'
AND cust_credit.CREDIT_CARD_NO = '4210653312478046'
ORDER BY cust_credit.TIMEID DESC


#QUERY 10: Which transaction type has a high rate of transactions.
SELECT DISTINCT(TRANSACTION_TYPE), COUNT(TRANSACTION_VALUE)
FROM cdw_sapp_credit
GROUP BY TRANSACTION_TYPE 
ORDER BY COUNT(TRANSACTION_VALUE) DESC


#QUERY 11: Which state has a high number of customers. 
SELECT CUST_STATE, COUNT(CREDIT_CARD_NO)
FROM cdw_sapp_custmer
GROUP BY CUST_STATE
ORDER BY COUNT(CREDIT_CARD_NO) DESC


#QUERY 12: Sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
SELECT CUST_SSN, ROUND(SUM(TRANSACTION_VALUE),2) AS Transaction_Amount
FROM cdw_sapp_credit
GROUP BY CUST_SSN
ORDER BY Transaction_Amount DESC
LIMIT 10


#QUERY 13: Top three months with the largest transaction data
SELECT SUBSTRING(TIMEID, 5,2) AS MONTH, ROUND(SUM(TRANSACTION_VALUE),2) AS Transaction_Amount
FROM cdw_sapp_credit
GROUP BY SUBSTRING(TIMEID, 5,2)
ORDER BY Transaction_Amount DESC


#QUERY 14: Which branch processed the highest total dollar value of healthcare transactions.
SELECT branch_credit.BRANCH_CODE AS Branch_Code, ROUND(SUM(branch_credit.TRANSACTION_VALUE),2) AS Transaction_Amount
FROM (
	SELECT credit.TRANSACTION_TYPE, credit.TRANSACTION_VALUE, branch.*
	FROM cdw_sapp_credit AS credit
	JOIN cdw_sapp_branch AS branch
	ON credit.BRANCH_CODE = branch.BRANCH_CODE
	) AS branch_credit
WHERE TRANSACTION_TYPE = 'Healthcare'
GROUP BY Branch_Code
ORDER BY Transaction_Amount DESC


#QUERY 15: We need to find the Branch who made the maximum number of Transactions each day.
#If more than one Branch has the max(number of Transactions) return the lowest Branch value
SELECT t1.BRANCH_CODE, t1.TRANSACTION_DATE, COUNT(*) AS NUM_TRANSACTIONS
FROM (
		SELECT credit.TRANSACTION_TYPE, credit.TIMEID AS TRANSACTION_DATE, branch.*
		FROM cdw_sapp_credit AS credit
		JOIN cdw_sapp_branch AS branch
		ON credit.BRANCH_CODE = branch.BRANCH_CODE
		) AS t1
INNER JOIN #Step 4: Join t3 and t4 to filter out only rows with max transactions count per day
	(	#Step 3: find max count per day
		SELECT t2.TRANSACTION_DATE, MAX(TRANSACTION_COUNT) AS MAX_TRANSACTION_COUNT
		FROM (	#Step 2: count number of transactions per branch on each day
					SELECT branch_credit.BRANCH_CODE AS BRANCH, branch_credit.TRANSACTION_DATE, COUNT(*) TRANSACTION_COUNT
					FROM ( 	#Step 1: Join cdw_sapp_credit & cdw_sapp_branch tables
								SELECT credit.TRANSACTION_TYPE, credit.TIMEID AS TRANSACTION_DATE, branch.*
								FROM cdw_sapp_credit AS credit
								JOIN cdw_sapp_branch AS branch
								ON credit.BRANCH_CODE = branch.BRANCH_CODE
							) AS branch_credit
					GROUP BY branch_credit.BRANCH_CODE, branch_credit.TRANSACTION_DATE
				) AS t2
		GROUP BY t2.TRANSACTION_DATE
	) t3
ON 
t1.TRANSACTION_DATE = t3.TRANSACTION_DATE AND t1.BRANCH_CODE = (
	SELECT t4.BRANCH_CODE
	FROM (
			SELECT credit.TRANSACTION_TYPE, credit.TIMEID AS TRANSACTION_DATE, branch.*
			FROM cdw_sapp_credit AS credit
			JOIN cdw_sapp_branch AS branch
			ON credit.BRANCH_CODE = branch.BRANCH_CODE
			) AS t4
	WHERE t4.TRANSACTION_DATE = t1.TRANSACTION_DATE
	GROUP BY t4.BRANCH_CODE
	HAVING COUNT(*) = t3.MAX_TRANSACTION_COUNT
	ORDER BY t4.BRANCH_CODE ASC
	LIMIT 1
	)
GROUP BY t1.BRANCH_CODE, t1.TRANSACTION_DATE	


#QUERY 16: Display number of Transactions for each Branch. Sorted by number of transactions in ascending order, then Branch in ascending order
SELECT branch_credit.BRANCH, COUNT(*) AS TRANSACTION_COUNT
FROM
	(
	SELECT branches.BRANCH_CODE AS BRANCH, credit.*
	FROM cdw_sapp_credit AS credit
	JOIN cdw_sapp_branch AS branches
	ON credit.BRANCH_CODE = branches.BRANCH_CODE
	) AS branch_credit
GROUP BY BRANCH
ORDER BY TRANSACTION_COUNT ASC, BRANCH ASC


#QUERY 17: Which Transaction Type is most popular with Branch. Sorted by number of branches in descending order, then transaction type in ascending order.
SELECT branch_credit.TRANSACTION_TYPE AS TRANSACTION_TYPE, COUNT(*) AS NUMBER_OF_BRANCHES
FROM	(
	SELECT branch.*, credit.TRANSACTION_TYPE
	FROM cdw_sapp_branch AS branch
	JOIN cdw_sapp_credit AS credit
	ON branch.BRANCH_CODE = credit.BRANCH_CODE
	) AS branch_credit
GROUP BY TRANSACTION_TYPE
ORDER BY NUMBER_OF_BRANCHES DESC, TRANSACTION_TYPE ASC


#QUERY 18: Which Month were did branches proccess the highest total dollar value of Bill Transactions.
SELECT SUBSTRING(branch_credit.TIMEID, 5,2) AS MONTH, COUNT(*) AS TRANSACTION_COUNT
FROM (
	SELECT credit.*
	FROM cdw_sapp_credit AS credit
	JOIN cdw_sapp_branch AS branch
	ON credit.BRANCH_CODE = branch.BRANCH_CODE
	) AS branch_credit
WHERE branch_credit.TRANSACTION_TYPE = 'Bills'
GROUP BY MONTH
ORDER BY TRANSACTION_cOUNT DESC



#QUERY 19: Percentage of each Transaction Type processing per Month.
SELECT T1.MONTH, T1.TRANSACTION_TYPE, COUNT(*) AS TOTAL_COUNT, COUNT(*)*100/MONTH_TOTAL AS TRANSACTION_PERCENTAGE
FROM
	(
		SELECT SUBSTRING(TIMEID,5,2) AS MONTH, TRANSACTION_TYPE
		FROM cdw_sapp_credit ) AS T1
INNER JOIN
	(
		SELECT SUBSTRING(TIMEID,5,2) AS MONTH, COUNT(*) AS MONTH_TOTAL
		FROM cdw_sapp_credit
		GROUP BY SUBSTRING(TIMEID,5,2)) AS T2
ON T1.MONTH =T2.MONTH
GROUP BY T1.MONTH, T1.TRANSACTION_TYPE



#QUERY 20: Highest Transaction Type for each State for Branches.
SELECT T2.BRANCH_STATE, TRANSACTION_COUNT, T2.TRANSACTION_TYPE
FROM	(#Step 2: table 2 with a transaction counts per state
		SELECT branch_credit.TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT, branch_credit.BRANCH_STATE
		FROM	(
					SELECT credit.*, branch.BRANCH_STATE
					FROM cdw_sapp_credit AS credit
					JOIN cdw_sapp_branch AS branch
					ON credit.BRANCH_CODE = branch.BRANCH_CODE
					) AS branch_credit
		GROUP BY branch_credit.BRANCH_STATE, branch_credit.TRANSACTION_TYPE
		) AS T2
INNER JOIN 
(#Step 1: table 1 with all max transaction count per state
	SELECT MAX(TRANSACTION_COUNT) AS MAX_COUNT, T1.BRANCH_STATE, T1.TRANSACTION_TYPE
	FROM	(	
			SELECT branch_credit.TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT, branch_credit.BRANCH_STATE
			FROM	(
					SELECT credit.*, branch.BRANCH_STATE
					FROM cdw_sapp_credit AS credit
					JOIN cdw_sapp_branch AS branch
					ON credit.BRANCH_CODE = branch.BRANCH_CODE
					) AS branch_credit
			GROUP BY branch_credit.BRANCH_STATE, branch_credit.TRANSACTION_TYPE
	) T1
	GROUP BY T1.BRANCH_STATE
) T3
ON T2.BRANCH_STATE =T3.BRANCH_STATE AND T2.TRANSACTION_COUNT = T3.MAX_COUNT 
#Inner Join to match transaction type to match max transaction count




#QUERY 21: Lowest Transaction Type for each State for Branches.
SELECT T2.BRANCH_STATE, TRANSACTION_COUNT, T2.TRANSACTION_TYPE
FROM	(#Step 2: table 2 with a transaction counts per state
		SELECT branch_credit.TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT, branch_credit.BRANCH_STATE
		FROM	(
					SELECT credit.*, branch.BRANCH_STATE
					FROM cdw_sapp_credit AS credit
					JOIN cdw_sapp_branch AS branch
					ON credit.BRANCH_CODE = branch.BRANCH_CODE
					) AS branch_credit
		GROUP BY branch_credit.BRANCH_STATE, branch_credit.TRANSACTION_TYPE
		) AS T2
INNER JOIN 
(#Step 1: table 1 with all max transaction count per state
	SELECT MIN(TRANSACTION_COUNT) AS MIN_COUNT, T1.BRANCH_STATE, T1.TRANSACTION_TYPE
	FROM	(	
			SELECT branch_credit.TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT, branch_credit.BRANCH_STATE
			FROM	(
					SELECT credit.*, branch.BRANCH_STATE
					FROM cdw_sapp_credit AS credit
					JOIN cdw_sapp_branch AS branch
					ON credit.BRANCH_CODE = branch.BRANCH_CODE
					) AS branch_credit
			GROUP BY branch_credit.BRANCH_STATE, branch_credit.TRANSACTION_TYPE
	) T1
	GROUP BY T1.BRANCH_STATE
) T3
ON T2.BRANCH_STATE =T3.BRANCH_STATE AND T2.TRANSACTION_COUNT = T3.MIN_COUNT 
#Inner Join to match transaction type to match min transaction count


#QUERY 22: Highest Transaction Type for each Month for Branches.
SELECT T2.MONTH,T2.TRANSACTION_TYPE, MAX_COUNT 
FROM
	(	
		SELECT COUNT(*) AS TRANSACTION_COUNT, branch_credit.TRANSACTION_TYPE AS TRANSACTION_TYPE, MONTH
		FROM
		(
			SELECT SUBSTRING(credit.TIMEID, 5,2) AS MONTH, credit.TRANSACTION_TYPE, branch.*
			FROM cdw_sapp_credit AS credit
			JOIN cdw_sapp_branch AS branch
			ON credit.BRANCH_CODE = branch.BRANCH_CODE
		) AS branch_credit
		GROUP BY MONTH, TRANSACTION_TYPE
	) AS T2	
INNER JOIN
(	SELECT MAX(T1.TRANSACTION_COUNT) AS MAX_COUNT, T1.MONTH
	FROM 
		(
			SELECT COUNT(*) AS TRANSACTION_COUNT, branch_credit.TRANSACTION_TYPE AS TRANSACTION_TYPE, MONTH
			FROM
			(
				SELECT SUBSTRING(credit.TIMEID, 5,2) AS MONTH, credit.TRANSACTION_TYPE, branch.*
				FROM cdw_sapp_credit AS credit
				JOIN cdw_sapp_branch AS branch
				ON credit.BRANCH_CODE = branch.BRANCH_CODE
			) AS branch_credit
		GROUP BY TRANSACTION_TYPE, MONTH
 		) AS T1
	GROUP BY T1.MONTH
) AS T3
ON T3.MAX_COUNT=T2.TRANSACTION_COUNT



#QUERY 23: Stored procedure for percentage of each Transaction Type processing per Month.
DELIMITER //
CREATE PROCEDURE GetTransactionTypePercentageByMonth()
LANGUAGE SQL
BEGIN
	SELECT T1.MONTH, T1.TRANSACTION_TYPE, COUNT(*) AS TOTAL_COUNT, COUNT(*)*100/MONTH_TOTAL AS TRANSACTION_PERCENTAGE
	FROM
		(
			SELECT SUBSTRING(TIMEID,5,2) AS MONTH, TRANSACTION_TYPE
			FROM cdw_sapp_credit ) AS T1
	INNER JOIN
		(
			SELECT SUBSTRING(TIMEID,5,2) AS MONTH, COUNT(*) AS MONTH_TOTAL
			FROM cdw_sapp_credit
			GROUP BY SUBSTRING(TIMEID,5,2)) AS T2
	ON T1.MONTH =T2.MONTH
	GROUP BY T1.MONTH, T1.TRANSACTION_TYPE;
END //

DELIMITER ;

CALL GetTransactionTypePercentageByMonth();


#QUERY 24: Create Stored Procedure to insert data into a table
DELIMITER //
CREATE PROCEDURE INSERT_DATA(
IN CREDIT_CARD_NO TEXT, 
IN CUST_CITY TEXT, IN CUST_COUNTRY TEXT,
IN CUST_EMAIL TEXT,
IN CUST_PHONE VARCHAR(14), 
IN CUST_STATE TEXT, 
IN CUST_ZIP TEXT, 
IN FIRST_NAME TEXT, 
IN LAST_NAME TEXT,
IN LAST_UPDATED TEXT,
IN MIDDLE_NAME TEXT, 
IN SSN BIGINT, 
IN FULL_STREET_ADDRESS VARCHAR(50)
)
BEGIN
INSERT INTO cdw_sapp_custmer (
	CREDIT_CARD_NO, CUST_CITY, CUST_COUNTRY, CUST_EMAIL, CUST_PHONE, CUST_STATE, CUST_ZIP, 
	FIRST_NAME, LAST_NAME, LAST_UPDATED, MIDDLE_NAME, SSN, FULL_STREET_ADDRESS
) VALUES (
	CREDIT_CARD_NO, CUST_CITY, CUST_COUNTRY, CUST_EMAIL, CUST_PHONE, CUST_STATE, CUST_ZIP,
	FIRST_NAME, LAST_NAME, LAST_UPDATED, MIDDLE_NAME, SSN, FULL_STREET_ADDRESS
	);
END //
DELIMITER ;

#Insert data into table using INSERT_DATA()
CALL INSERT_DATA('1231231231231231', 'Dallas', 'United States', 'mayacampbell@perscholas.org', '(516)111-1111',
'TX', '11111', 'Maya', 'Campbell', NOW(), 's', 123456789, '123 Perscholas Street')
#NOW() used to get datetime

#Check to see if data was inserted
SELECT *
FROM cdw_sapp_custmer
WHERE FIRST_NAME = 'Maya'
#QUERY 25: Used to drop procedure
DROP PROCEDURE IF EXISTS UPDATE_CUST_PHONE;


#QUERY 26: Create Stored Procedure to Update Table
DELIMITER //
CREATE PROCEDURE UPDATE_CUST_PHONE(
IN P_SSN BIGINT, 
IN P_NEW_CUST_PHONE VARCHAR(14)
)
LANGUAGE SQL
BEGIN
	UPDATE cdw_sapp_custmer
	SET CUST_PHONE = P_NEW_CUST_PHONE 
	WHERE SSN =P_SSN;
END //
DELIMITER ;

CALL UPDATE_CUST_PHONE(123456100, '(111)111-1111');

SELECT*
FROM cdw_sapp_custmer
WHERE SSN = '123456100'

#QUERY 27: Create Stored Procedure to Update Transaction Price
DELIMITER //
CREATE PROCEDURE UPDATE_PRICE(
IN P_TRANSACTION_ID BIGINT,
IN INCREMENT INT
)
LANGUAGE SQL
BEGIN
	UPDATE cdw_sapp_credit
	SET TRANSACTION_VALUE=TRANSACTION_VALUE+INCREMENT
	WHERE TRANSACTION_ID=P_TRANSACTION_ID;
END //
DELIMITER ;

CALL UPDATE_PRICE(1, 10);

SELECT*
FROM cdw_sapp_credit
WHERE TRANSACTION_ID = 1

#QUERY 28: Create Stored Procedure to Select Data from Table based on given Transaction type.
DELIMITER //
CREATE PROCEDURE SELECT_DATA(
IN P_TRANSACTION_TYPE TEXT
)
LANGUAGE SQL
BEGIN
	SELECT *
	FROM cdw_sapp_credit
	WHERE TRANSACTION_TYPE = P_TRANSACTION_TYPE;
END //

DELIMITER ;

CALL SELECT_DATA('Bills')

#QUERY 29: Create Stored Procedure to Delete Data from Table
DELIMITER //
CREATE PROCEDURE DELETE_DATA(
IN P_FIRST_NAME TEXT
)
LANGUAGE SQL
BEGIN
	DELETE
	FROM cdw_sapp_custmer
	WHERE FIRST_NAME = P_FIRST_NAME;
END //
DELIMITER ;
	
CALL DELETE_DATA('Maya')

SELECT *
FROM cdw_sapp_custmer
WHERE FIRST_NAME ='Maya'

#QUERY 30: Create Stored Procedure to Validate SSN
DELIMITER //
CREATE PROCEDURE VERIFY(
IN P_SSN BIGINT)
BEGIN
	IF EXISTS(SELECT * FROM cdw_sapp_custmer WHERE SSN = P_SSN) THEN
		SELECT 'USER EXISTS';
	ELSE
		SELECT 'USER DOES NOT EXISTS';
	END IF;
END //
DELIMITER ;

CALL VERIFY(123456100);

#QUERY 31: Create Stored Procedure to check account details of an existing customer
DELIMITER //
CREATE PROCEDURE DISPLAY_ACCOUNT_DETAILS (
IN P_FIRST_NAME TEXT, 
IN P_LAST_NAME TEXT)

BEGIN 
	DECLARE NAME VARCHAR(50);
	DECLARE ADDRESS VARCHAR(200);
	DECLARE EMAIL VARCHAR(100);
	DECLARE PHONE_NUMBER VARCHAR(14);
	DECLARE CC_NO VARCHAR(17);
	
	IF EXISTS(SELECT * FROM cdw_sapp_custmer  WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME) THEN
		SET NAME= (SELECT CONCAT(FIRST_NAME,' ', MIDDLE_NAME,' ' ,LAST_NAME) FROM cdw_sapp_custmer WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME);
		SET ADDRESS= (SELECT CONCAT(FULL_STREET_ADDRESS, ' ', CUST_CITY, ' , ', CUST_STATE, ' ', CUST_ZIP) FROM cdw_sapp_custmer WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME);
		SET EMAIL = (SELECT CUST_EMAIL FROM cdw_sapp_custmer WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME);
		SET PHONE_NUMBER = (SELECT CUST_PHONE FROM cdw_sapp_custmer WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME);
		SET CC_NO = (SELECT CREDIT_CARD_NO FROM cdw_sapp_custmer WHERE FIRST_NAME = P_FIRST_NAME AND LAST_NAME=P_LAST_NAME);
		SELECT CONCAT('Customer Name is: ', NAME) AS 'ACCOUNT DETAILS' UNION ALL
		SELECT CONCAT('Customer Address is: ',  ADDRESS) UNION ALL
		SELECT CONCAT('Customer Email Address is: ',  EMAIL) UNION ALL
		SELECT CONCAT('Customer Phone Number is: ',  PHONE_NUMBER) UNION ALL
		SELECT CONCAT('Customer Credit Card Number is: ',  CC_NO);
	ELSE
		SELECT 'USER DOES NOT EXIST';
	END IF;
END //
DELIMITER ;

CALL DISPLAY_ACCOUNT_DETAILS('Alec', 'Hooper');


#QUERY 32: Create Stored Procedure to Modify existing account details of a customer.
DELIMITER //
CREATE PROCEDURE MODIFY_EXISTING_ACCOUNT(
IN P_SSN BIGINT,
IN P_NEW_EMAIL TEXT,
IN P_NEW_PHONE_NUMBER VARCHAR(14)
)
LANGUAGE SQL
BEGIN
	DECLARE V_EXISTING_RECORDS INT;
	DECLARE V_UPDATED_RECORDS INT;
	
	SELECT COUNT(*) INTO V_EXISTING_RECORDS FROM cdw_sapp_custmer WHERE SSN = P_SSN;
	
	IF V_EXISTING_RECORDS =0 THEN
		SELECT 'Customer does not exist' AS MESSAGE;
	ELSE
		UPDATE cdw_sapp_custmer SET CUST_EMAIL = P_NEW_EMAIL, CUST_PHONE = P_NEW_PHONE_NUMBER WHERE SSN = P_SSN;
		SELECT 'Account details updated successfully' AS MESSAGE;
	END IF;
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS MODIFY_EXISTING_ACCOUNT

CALL MODIFY_EXISTING_ACCOUNT(123456100,'alecwhooper@example.com','(222)222-2222');

SELECT *
FROM cdw_sapp_custmer
WHERE SSN =123456100


#QUERY 33: Create Stored Procedure for insert and update with output parameter
CREATE PROCEDURE UPDATE_CUSTOMER(
#Input parameters
IN P_CC_NO TEXT,
IN P_CITY TEXT, 
IN P_COUNTRY TEXT,
IN P_CUST_EMAIL TEXT,
IN P_PHONE_NUMBER VARCHAR(14),
IN P_ZIP TEXT,
IN P_FIRST_NAME TEXT,
IN P_LAST_NAME TEXT, 
IN P_LAST_UPDATED TEXT, 
IN P_MIDDLE_NAME TEXT,
IN P_SSN BIGINT,
IN P_FULL_STREET_ADDRESS VARCHAR(50),
#Output parameter
IN P_MESSAGE VARCHAR(30)
)
LANGUAGE SQL
BEGIN
	DECLARE V_ROWCOUNT INT; #Declare variable to hold thhe count of rows with the given ssn
	#Check if there is an existing row with given ssn
	SELECT COUNT(*) INTO V_ROWCOUNT FROM cdw_sapp_custmer WHERE SSN = P_SSN;
	#if there is a row with given ssn, update customer detais
	IF V_ROWCOUNT > 0 THEN
		SET P_MESSAGE = 'ROW UPDATED'; #set output parameter to show that row was updates
		UPDATE cdw_sapp_custmer
		SET CREDIT_CARD_NO = P_CC_NO, 
			CUST_CITY = P_CITY, 
			CUST_COUNTRY = P_COUNTRY,
			CUST_PHONE = P_PHONE_NUMBER, 
			CUST_ZIP = P_ZIP, 
			FIRST_NAME = P_FIRST_NAME, 
			LAST_NAME = P_LAST_NAME, 
			LAST_UPDATED = P_LAST_UPDATED, 
			MIDDLE_NAME = P_MIDDLE_NAME, 
			SSN = P_SSN, 
			FULL_STREET_ADDRESS = P_FULL_STREET_ADDRESS
		WHERE SSN = P_SSN;
	#If there is no row with given ssn, insert a new customer with input values
	ELSE 
		SET P_MESSAGE='ROW INSERTED'; #set output parameter to show row was inserted
		INSERT INTO cdw_sapp_custmer(CREDIT_CARD_NO, CUST_CITY, CUST_COUNTRY, CUST_PHONE, CUST_ZIP, FIRST_NAME,
		LAST_NAME, LAST_UPDATED, MIDDLE_NAME, SSN, FULL_STREET_ADDRESS)
		VALUES(P_CC_NO, P_CITY, P_COUNTRY, P_PHONE_NUMBER, P_ZIP, P_FIRST_NAME, P_LAST_NAME, P_LAST_UPDATED, 
		P_MIDDLE_NAME, P_SSN, P_FULL_STREET_ADDRESS);
	END IF;
END //

DELIMITER;

SET @MessageReturned = '';
CALL UPDATE_CUSTOMER('4444653310061055', 'Boston', 'United States', 'mscampbell@example.com', 'NC', '12332', 'Maya', 'Campbell', NOW(), 'Sh', 222345678, '26, Main Street North', @MessageReturned);
SELECT @MessageReturned;

USE MASTER 
