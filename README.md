# SQL_Exploritory
# OVERVIEW
This project provided me with the opportunity to explore multiple datasetes in a relational database using various forms of SQL commands.  This project is my opportunity to demonstrate the knowledge and abilities I have learned throughout my 4 month Data Engineering Bootcamp through Per Scholas.  As well as demonstrate my ability to read and comprehend documentation.  I worked with the following technologies to manage an ETL process for a Credit Card Dataset: Python, MariaDB, Apache Spark.

# STEP 1:
To begin I imported the following Python modules into my py script file: findspark, pyspark.sql and pyspark.sql.functions, pandas, and os.  Then I created a Spark Session and assigned it to my spark variable.  Using spark.read.json() to read my json files and assigning each one to a different variable.  I then used my system variables to hide my MYSQL password and user name, used os.environ.get() to get my username and password and assign it to variables.  Then I created a MYSQL connection using sparks .write.format() and calling my username and password variables instead of my actual username and password so I coult connect to MARIADB.
