-- Data Analysis with Apache Hive on Cold Calls Data

-- Problem 1: Data Loading

-- a. make a directory named input_data:
-- hdfs dfs -mkdir input_data 

-- b. make a directory named Telecom in input_data: 
-- hdfs dfs -mkdir /input_data/car_insurance

-- c. load car_insurance_cold_calls_dataset.csv file in telecom directory
-- hdfs dfs -put car_insurance_cold_calls_dataset.csv /input_data/car_insurance/

-- we are creating Internal (Managed) Table 


create external table car_insurance
( Id int , Age int , 
Job string ,Marital string , 
Education string , Default int , 
Balance int , HHInsurance int ,
Carloan int , Communication string , 
LastContactDay int , LastContactMonth string, 
NoOfContacts int,  DaysPassed int, 
PrevAttempts int ,Outcome string , 
CallStart string, CallEnd string , 
CarInsurance int)
row format delimited
fields terminated by ','
STORED AS TEXTFILE 
LOCATION '/input_data/car_insurance/'
tblproperties ("skip.header.line.count" = "1");


-- Problem 2: Data Exploration

-- 1. How many records are there in the dataset?

SELECT COUNT(*) FROM car_insurance;

-- 2. How many unique job categories are there?

SELECT DISTINCT job FROM car_insurance;

-- 3. What is the age distribution of customers in the dataset? Provide a breakdown by age group: 18-30, 31-45, 46-60, 61+


SELECT 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance 
GROUP BY 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;



-- 4. Count the number of records that have missing values in any field

SELECT COUNT(*)
FROM car_insurance
WHERE Id IS NULL
OR Age IS NULL
OR Job IS NULL
OR Marital IS NULL
OR Education IS NULL
OR Default IS NULL
OR Balance IS NULL
OR HHInsurance IS NULL
OR CarLoan IS NULL
OR Communication IS NULL
OR LastContactDay IS NULL
OR LastContactMonth IS NULL
OR NoOfContacts IS NULL
OR DaysPassed IS NULL
OR PrevAttempts IS NULL
OR Outcome IS NULL
OR CallStart IS NULL
OR CallEnd IS NULL
OR CarInsurance IS NULL;

-- 5. Determine the number of unique 'Outcome' values and their respective counts

SELECT Outcome, COUNT(*) FROM car_insurance 
GROUP BY Outcome;

-- 6. Find the number of customers who have both a car loan and home insurance.

SELECT COUNT(*)
FROM car_insurance
WHERE HHInsurance = 1
AND CarLoan = 1;




-- Problem 3: Aggregations

-- 1. What is the average, minimum, and maximum balance for each job category?

SELECT job, AVG(Balance) as Average , MIN(Balance) as Minimum, MAX(Balance) as Maximum
FROM car_insurance
GROUP BY job;

-- 2. Find the total number of customers with and without car insurance

SELECT 
COUNT(CASE WHEN CarInsurance = 0 then id END) as Without_Insurance, 
COUNT(CASE WHEN CarInsurance = 1 then id END) as With_Insurance
FROM car_insurance; 

-- 3. Count the number of customers for each communication type.

SELECT Communication,count(*)
FROM car_insurance
GROUP BY Communication;


-- 4. Calculate the sum of 'Balance' for each 'Communication' type


SELECT Communication,SUM(Balance)
FROM car_insurance
GROUP BY Communication;

-- 5. Count the number of 'PrevAttempts' for each 'Outcome' type


SELECT Outcome,SUM(PrevAttempts)
FROM car_insurance
GROUP BY Outcome; 


-- 6. Calculate the average 'NoOfContacts' for people with and without 'CarInsurance'.


SELECT Carinsurance, AVG(NoOfContacts)
FROM car_insurance
GROUP BY Carinsurance;


-- Problem 4: Partitioning and Bucketing

-- 1. Create a partitioned table on 'Education' and 'Marital' status. Load data from the original table to this new partitioned table.

create table car_insurance_partitioned
( Id int , Age int , 
	Job string , 
	Default int , 
	Balance int , HHInsurance int ,
	Carloan int , Communication string , 
	LastContactDay int , LastContactMonth string, 
	NoOfContacts int,  DaysPassed int, 
	PrevAttempts int ,Outcome string , 
	CallStart string, CallEnd string , 
	CarInsurance int)
	PARTITIONED BY (Education string, Marital string)
	ROW FORMAT DELIMITED 
	FIELDS TERMINATED BY ','
	STORED AS TEXTFILE;

INSERT OVERWRITE table 
car_insurance_partitioned Partition(Education,Marital)
SELECT Id,Age,Job,Default,Balance,HHInsurance,
CarLoan,Communication,LastContactDay,
LastContactMonth,NoOfContacts,DaysPassed,
PrevAttempts,Outcome,CallStart,CallEnd,
CallInsurance,Education,Marital
FROM car_insurance;


-- 2. Create a bucketed table on 'Age', bucketed into 4 groups (as per the age groups mentioned above). Load data from the original table into this bucketed table.


set hive.enforce.bucketing=true;

create table car_insurance_bucketed_age
(Id int,Age int,
Job string ,Marital string ,
Education string , Default int ,
Balance int , HHInsurance int ,
Carloan int , Communication string ,
LastContactDay int , LastContactMonth string,
NoOfContacts int,  DaysPassed int,
PrevAttempts int ,Outcome string ,
CallStart string, CallEnd string ,
CarInsurance int)
CLUSTERED BY (Age)
INTO 4 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;



INSERT OVERWRITE table car_insurance_bucketed_age SELECT * FROM car_insurance;

-- 3. Add an additional partition on 'Job' to the partitioned table created earlier and move the data accordingly.

create table car_insurance_partitioned_new(
Id int,
Age int,
Default int,
Balance int,
HHInsurance int,
Carloan int,
Communication string,
LastContactDay int,
LastContactMonth string,
NoOfContacts int,
DaysPassed int,
PrevAttempts int,
Outcome string,
CallStart string,
CallEnd string,
CarInsurance int)
PARTITIONED BY (Education string,Marital string,Job string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE table car_insurance_partitioned_new partition(Education,Marital,Job)
SELECT Id,Age,Job,Default,Balance,HHInsurance,
CarLoan,Communication,LastContactDay,
LastContactMonth,NoOfContacts,DaysPassed,
PrevAttempts,Outcome,CallStart,CallEnd,
CallInsurance,Education,Marital
FROM car_insurance;

-- 4. Increase the number of buckets in the bucketed table to 10 and redistribute the data.


create table car_insurance_bucketed_age_new
(Id int,Age int,
Job string ,Marital string ,
Education string , Default int ,
Balance int , HHInsurance int ,
Carloan int , Communication string ,
LastContactDay int , LastContactMonth string,
NoOfContacts int,  DaysPassed int,
PrevAttempts int ,Outcome string ,
CallStart string, CallEnd string ,
CarInsurance int)
CLUSTERED BY (Age)
INTO 10 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE table car_insurance_bucketed_age_new SELECT * FROM car_insurance;



-- 1. Join the original table with the partitioned table and find out the average 'Balance' for each 'Job' and 'Education' level

SELECT C.Job,N.Education, AVG(C.Balance)
FROM car_insurance C 
INNER JOIN car_insurance_partitioned_new N
ON N.id = C.id
GROUP BY C.Job, N.Education;


-- 2. Join the original table with the bucketed table and calculate the total 'NoOfContacts' for each 'Age' group.


SELECT B.Age,COUNT(C.NoOfContacts)
FROM car_insurance C 
INNER JOIN car_insurance_bucketed_age_new B 
ON C.id = B.id
GROUP BY B.Age;

-- 3. Join the partitioned table and the bucketed table based on the 'Id' field and find the total balance for each education level and marital status for each age group.

SELECT P.Education,P.Marital,B.Age,SUM(B.Balance) as Bal
FROM car_insurance_partitioned_new P 
INNER JOIN car_insurance_bucketed_age_new B 
ON P.id = B.id 
GROUP BY B.Age,P.Education,P.Marital;


-- Problem 6: Window Function

-- 1. Calulate the cumulative sum of 'NoOfContacts' for each 'Job' category, ordered by 'Age'.

SELECT Age,Job, 
SUM(NoOfContacts) OVER(PARTITION BY Job ORDER BY Age) as Cum_Sum
FROM car_insurance_bucketed_age_new
ORDER BY Age,Job;



-- 2. Calculate the running average of 'Balance' for each 'Job' category, ordered by 'Age'.


SELECT Age,Job,Balance, 
AVG(Balance) OVER(PARTITION BY Job ORDER BY Age) as Avg_Bal
FROM car_insurance_bucketed_age_new
ORDER BY Age,Job;


-- 3. For each 'Job' category, find the maximum 'Balance' for each 'Age' group using window functions

SELECT Age,Job,Balance FROM (
SELECT Age,Job,Balance,ROW_NUMBER() OVER(PARTITION BY Job,Age ORDER BY Balance DESC) as rn 
FROM car_insurance) t
WHERE rn = 1
ORDER BY Job,Age;


-- 4. Calculate the rank of 'Balance' within each 'Job' category, ordered by 'Balance' descending.


SELECT Job,Balance,
RANK() OVER(Partition by Job ORDER BY Balance DESC) as RNK
FROM car_insurance
ORDER BY Job,Balance DESC;



-- Problem 7: Advance Aggregation 


-- 1. Find the job category with the highest number of car insurances

SELECT Job,COUNT(CallInsurance) as Insurance_count
FROM car_insurance
WHERE CallInsurance = 1
GROUP BY Job
ORDER BY Insurance_count DESC 
LIMIT 1;

-- 2. Which month has seen the highest number of last contacts?

SELECT LastContactMonth, COUNT(LastContactMonth) as MonthCount
FROM car_insurance
GROUP BY LastContactMonth
ORDER BY MonthCount DESC 
LIMIT 1;



-- 3. Calculate the ratio of the number of customers with car insurance to the number of customers without car insurance for each job category.

SELECT Job,
ROUND(
	COUNT(CASE WHEN CallInsurance = 1 THEN 1 END)
	/COUNT(CASE WHEN CallInsurance = 0 THEN 1 END),2) AS Ratio 
FROM car_insurance
GROUP BY Job; 


-- 4. Find out the 'Job' and 'Education' level combination which has the highest number of car insurances.

SELECT Job,Education,
COUNT(CASE WHEN CallInsurance = 1 THEN 1 END) AS Insured 
FROM car_insurance
GROUP BY Job,Education
ORDER BY Insured DESC
LIMIT 1;


-- 5. Calculate the average 'NoOfContacts' for each 'Outcome' and 'Job' combination.

SELECT Outcome,Job,AVG(NoOfContacts) as AverageContacts
FROM car_insurance
GROUP BY Outcome,Job
ORDER BY  Outcome,Job;


-- 6. Determine the month with the highest total 'Balance' of customers.

SELECT LastContactMonth, SUM(Balance) as TotalBal
FROM car_insurance
GROUP BY LastContactMonth
ORDER BY TotalBal DESC
LIMIT 1;


-- Problem 8: Complex joins and aggregations

-- 1. For customers who have both a car loan and home insurance,find out the average 'Balance' for each 'Education' level


SELECT Education, AVG(Balance) as AverageBalance
FROM car_insurance
WHERE CallInsurance = 1 AND Carloan = 1
GROUP BY Education
ORDER BY AverageBalance;

-- 2. Identify the top 3 'Communication' types for customers with 'CarInsurance', and display their average 'NoOfContacts'.

SELECT Communication,AVG(NoOfContacts) as AverageContacts
FROM car_insurance
WHERE CallInsurance = 1
GROUP BY Communication
ORDER BY AverageContacts DESC 
LIMIT 3;


-- 3. For customers who have a car loan, calculate the average balance for each job category

SELECT Job, AVG(Balance) as AverageBalance
FROM car_insurance
WHERE CarLoan = 1
GROUP BY Job
ORDER BY AverageBalance;



-- 4. Identify the top 5 job categories that have the most customers with a 'default', and show their average 'balance'


SELECT Job, AVG(Balance) as AverageBalance
FROM car_insurance
WHERE Default = 1 
GROUP BY Job 
ORDER BY AverageBalance DESC
LIMIT 5;



-- Problem 9: Advanced Window Functions


-- 1. Calculate the difference in 'NoOfContacts' between each customer and the customer with the next highest number of contacts in the same 'job' category.


SELECT c1.Id,c1.Job,c1.NoOfContacts,
c1.NoOfContacts - c2.NextHighestContacts as ContactDifferencce
FROM car_insurance c1 
JOIN(
SELECT c1.Job, c1.NoOfContacts,
MIN(c2.NoOfContacts) AS
NextHighestContacts
FROM car_insurance c1
LEFT JOIN car_insurance c2 ON
c1.Job = c2.Job AND c1.NoOfContacts <
c2.NoOfContacts
GROUP BY c1.Job, c1.NoOfContacts) c2 
ON c1.Job = c2.Job AND c1.NoOfContacts = c2.NoOfContacts


-- 2. For each customer, calculate the difference between their 'balance' and the average 'balance' of their 'job' category.

SELECT j1.Id,j1.Job,j1.Balance, j1.Balance - j.AverageBalance as  BalanceDifference
from car_insurance j1 JOIN 
( SELECT Job, AVG(Balance) as AverageBalance 
FROM car_insurance
GROUP BY Job ) j
on j1.Job = j.Job;


-- 3. For each 'Job' category, find the customer who had the longest call duration.

SELECT Job,id, Duration FROM (SELECT Job,id, (CallStart - CallEnd) as Duration,
ROW_NUMBER() OVER(Partition BY Job ORDER BY (CallStart - CallEnd) DESC) as rn
FROM car_insurance) C
WHERE rn = 1;


-- 4. Calculate the moving average of 'NoOfContacts' within each 'Job' category, using a window frame of the current row and the two preceding rows.


SELECT Id, Job, NoOfContacts,
AVG(NoOfContacts) 
OVER(Partition BY Job 
ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as MovingAverage
FROM car_insurance;


-- Problem 10: Performance Tuning


-- 1. Experiment with different file formats (like ORC, Parquet) and measure their impact on the performance of your Hive queries.

# Parquet file format
create table car_insurance_parquet
( Id int , Age int , 
Job string ,Marital string , 
Education string , Default int , 
Balance int , HHInsurance int ,
Carloan int , Communication string , 
LastContactDay int , LastContactMonth string, 
NoOfContacts int,  DaysPassed int, 
PrevAttempts int ,Outcome string , 
CallStart string, CallEnd string , 
CarInsurance int)
stored as parquet;

from car_insurance insert overwrite table car_insurance_parquet select *;



create table car_insurance_orc
( Id int , Age int , 
Job string ,Marital string , 
Education string , Default int , 
Balance int , HHInsurance int ,
Carloan int , Communication string , 
LastContactDay int , LastContactMonth string, 
NoOfContacts int,  DaysPassed int, 
PrevAttempts int ,Outcome string , 
CallStart string, CallEnd string , 
CarInsurance int)
stored as orc;

from car_insurance insert overwrite table car_insurance_orc select *;

-- query testing:


SELECT 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance 
GROUP BY 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;

-- Time taken: 12.326 seconds, Fetched: 4 row(s)

SELECT 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_parquet 
GROUP BY 
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;



-- Time taken: 11.307 seconds, Fetched: 4 row(s)


SELECT
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_orc
GROUP BY
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;



-- Time taken: 10.562 seconds, Fetched: 4 row(s)



SELECT
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_partitioned
GROUP BY
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;


-- Time taken: 10.723 seconds, Fetched: 4 row(s)


SELECT
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_partitioned_new
GROUP BY
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;


-- Time taken: 11.249 seconds, Fetched: 4 row(s)

SELECT
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_bucketed_age
GROUP BY
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;


-- Time taken: 10.232 seconds, Fetched: 4 row(s)


SELECT
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END AS Age_Group,
COUNT(*) as count
FROM car_insurance_bucketed_age_new
GROUP BY
CASE WHEN age > 17 AND age < 31 THEN '18-30' 
WHEN age > 30 AND age < 46 THEN '31-45' 
WHEN age > 45 AND age < 61 THEN '46-60'
WHEN age > 60 THEN '61+' END;


-- Time taken: 10.106 seconds, Fetched: 4 row(s)




