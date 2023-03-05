# SQL_Exploritory
# OVERVIEW
This project provided me with the opportunity to explore multiple datasetes in a relational database using various forms of SQL commands.  This project is also my opportunity to demonstrate the knowledge and abilities I have learned throughout my 4 month Data Engineering Bootcamp through Per Scholas.  As well as demonstrate my ability to read and comprehend documentation.  I worked with the following technologies to manage an ETL process for a Credit Card Dataset: Python, MariaDB, Apache Spark.

The Credit Card System database is a system developed for managing activities such as registering new customers and approving or canceling requests.  Below are the three files that contain the customer's transaction information and inventories in the credit card information that was used in this project:
- CDW_SAPP_CUSTOMER.JSON: This file contains existing customer details
- CDW_SAPP_CREDITCARD.JSON: This file contains information about credit card transactions
- CDW_SAPP_BRANCH.JSON: This file contains the information of each branch

# STEP 1:
Since I wanted to perform majority of this project in SQL I first loaded the dtat into MARIADB.  To begin I imported the following Python modules into my py script file: findspark, pyspark.sql and pyspark.sql.functions, pandas, and os.  Then I created a Spark Session and assigned it to my spark variable.  Using spark.read.json() to read my json files and assigning each one to a different variable.  I then used my system variables to hide my MYSQL password and user name, used os.environ.get() to get my username and password and assign it to variables.  Then I created a MYSQL connection using sparks .write.format() and calling my username and password variables instead of my actual username and password so I could connect to MARIADB.

# STEP 2:
Once data was loaded into database I begin transforming the data based on the mapping document.
