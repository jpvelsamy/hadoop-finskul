# hadoop-finskul
Contains tutorial code

Check out the code using git and create an eclipse template using maven and import the project. This code will work only with the bonsai cluster that you need to setup using the hadoop docker projects present as other repositories under my account.

1. Used maven archetype
mvn archetype:generate -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false -DgroupId=finskul -DartifactId=com.finskul.hadoop

2. Added hadoop 2.7.1 dependencies
Refere pom.xml for knowing the dependency list

3. Added eclipse template
mvn eclipse:eclipse

HIVE SESSION SCRIPTS

EXTERNAL TABLE
------------------------------------
CREATE EXTERNAL TABLE WHITEGOODS
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/jpvel/whitegoods/';


MANAGED TABLE
------------------------------------
./bin/hadoop fs -mkdir -p /user/jpvel/managed/whitegoods
./bin/hadoop fs -put /tmp/whitegoods.csv /user/jpvel/managed/whitegoods/lg.csv
./bin/hadoop fs -chown -R jpvel:jpvel /user/jpvel
./bin/hadoop fs -ls -R /user/jpvel/

CREATE TABLE finskul.WHITEGOODS
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/jpvel/managed/whitegoods';

EXTERNAL TABLES WITH SEQUENCE FILE
------------------------------------------------------------------------
CREATE EXTERNAL TABLE SEQ_WHITEGOODS
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS sequencefile 
LOCATION '/user/jpvel/sequence/whitegoods';

EXTERNAL TABLES WITH PARTITIONS
------------------------------------------------------------------------
CREATE EXTERNAL TABLE FINSKUL.WHITEGOODS_WITH_PARTN
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
PARTITIONED BY (MFG STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/jpvel/whitegoods/';

ALTER TABLE FINSKUL.WHITEGOODS_WITH_PARTN ADD PARTITION (MFG='LG') 
location '/user/jpvel/whitegoods/LG'



LOAD DATA FROM FILE
------------------------------------------------------------------------

CREATE TABLE finskul.WHITEGOODS_load
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


 LOAD DATA INPATH '/user/jpvel/whitegoods/SAMSUNG/samsung_part.csv' INTO TABLE finskul.WHITEGOODS_load;

 LOAD DATA LOCAL INPATH '/tmp/whitegoods.csv' INTO TABLE finskul.WHITEGOODS_load;


-CTAS STATEMENT - DATA FROM TEXT FILE INTO SEQUENCE FILE
 CREATE TABLE finskul.PRODUCT_LABEL
 (PRODUCT STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS sequencefile;

insert into finskul.PRODUCT_LABEL SELECT PRODUCT FROM finskul.WHITEGOODS_load;

CREATE TABLE finskul.MODEL_CAT AS SELECT MODEL, CATEGORY FROM FINSKUL.WHITEGOODS_WITH_PARTN;


CREATE  TABLE FINSKUL.WHITEGOODS_WITH_PARTN_INTERNAL
(PRODUCT STRING,
MODEL STRING,
CATEGORY STRING,
DP DOUBLE,
MRP DOUBLE,
WHP DOUBLE) 
PARTITIONED BY (MFG STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE FINSKUL.WHITEGOODS_WITH_PARTN_INTERNAL PARTITION(MFG) SELECT PRODUCT, MODEL, CATEGORY, DP, MRP, WHP, MFG FROM FINSKUL.MG_WHITEGOODS_TOTAL;

 SELECT COUNT(*) AS COUNT, MFG FROM FINSKUL.WHITEGOODS_WITH_PARTN_INTERNAL GROUP BY MFG;

 JOINS
 ------------------------------------
CREATE TABLE FINSKUL.MFG_MASTER (name STRING, rating INT) CLUSTERED BY (name) INTO 2 BUCKETS STORED AS ORC;
 
INSERT INTO TABLE FINSKUL.MFG_MASTER VALUES ('LG', 5), ('SAMSUNG', 4);

SELECT SUM(a.MRP), b.name, b.rating  FROM  FINSKUL.WHITEGOODS_WITH_PARTN_INTERNAL a JOIN  FINSKUL.MFG_MASTER b ON (a.MFG = b.name) GROUP BY b.name, b.rating;


PYTHON CLIENT
sudo apt-get build-dep python-numpy
sudo pip install numpy
sudo apt-get install libsasl2-dev
sudo pip install pyhs2
sudo pip install sasl
sudo pip install thrift
sudo pip install thrift-sasl
sudo pip install PyHive



PIG SAMPLES
sudo docker run -i -t -h bonsai-pig  ipedrazas/hadoop-pig /etc/bootstrap.sh -bash


Start the history server
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver


Pig wordcount  script
lines = LOAD '/user/root/pig/curation.final.amazon.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;

A sample with a join
drivers = LOAD '/user/root/pig/drivers.csv' USING PigStorage(',');
raw_drivers = FILTER drivers BY $0>1;
drivers_details = FOREACH raw_drivers GENERATE $0 AS driverId, $1 AS name;

timesheet = LOAD '/user/root/pig/timesheet.csv' USING PigStorage(',');
raw_timesheet = FILTER timesheet by $0>1;
timesheet_logged = FOREACH raw_timesheet GENERATE $0 AS driverId, $2 AS hours_logged, $3 AS miles_logged;

grp_logged = GROUP timesheet_logged by driverId;
sum_logged = FOREACH grp_logged GENERATE group as driverId, SUM(timesheet_logged.hours_logged) as sum_hourslogged, SUM(timesheet_logged.miles_logged) as sum_mileslogged;

join_sum_logged = JOIN sum_logged by driverId, drivers_details by driverId;
join_data = FOREACH join_sum_logged GENERATE $0 as driverId, $4 as name, $1 as hours_logged, $2 as miles_logged;
dump join_data;



