# S3-Spark-Project
## David Miler

## Running the Python Script in Command Line Tools
1. Navigate to the directory where the project is located
2. Simply run `python etl.py` to grab data from Udacity's S3 bucket and store it in mine

## Part I - Connecting to AWS
* This actually proved a bit difficult and required some debugging as the method to connect to AWS via PySpark has changed since Udacity published the program
* In the information provided by Udacity, simply establishing the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as environment variables stored in the `dl.cfg` file would give a developer access to S3 using a `SparkSession` object, but this is not the case
* Specifically, the developer needs to create a `sparkContext` object from the session, then point the access and secret keys to S3 by manually setting them in the context object's `hadoopConfiguration`

## Part II - Processing the Data
* For the raw songs data, processing was pretty straightforward. All that needed to be done was splliting it into an `artists` and `songs` table, dropping duplicate values from both then partitioning `songs` by the fields indicated in the instructions (artist and year)
* Working with the log data was significantly more complex
    * First the data needed to be filtered on user events playing songs (i.e. `page == 'NextSong'`)
    * Next we had to create a `udf` function that converted UNIX timestamps to datetime objects
    * With this we then created a time table and derived various day and date parts for partitioning/aggregation purposes
    * The raw songs data then had to be read back in and joined onto `logs` so we could establish all of the fields we wanted in the `songplays` table
    * Additionally the newly created `times` table was joined to bring in year and month fields, which were used for partioning
    * Lastly this table required a unique ID and since it didn't come with it, they had to derived using PySpark's built-in SQL function `monotonically_increasing_id`

## Part III - Making the Code More Pythonic
* As an added challenge, I wanted to utilize Python in its best way i.e. object oriented programming (OOP) and stored all of the functions in the `DataProcessor` class
* I wanted to do this primarily for modularity as well as debugging purposes
    * By making the ETL process class-based, a user/developer can pass in different input and output paths if they wanted to pull and push data in different places with minimal changes to the code
        * This is mostly about changing the destination of the data (i.e. to your own S3 bucket) as the code will only work on data specifically structured in the same way as Udacity's (Sparkify)
        * Additionally, once an instance of the class is created, a number of attributes are established on the object that can be referred to at any time to debug
            * This includes the input and output paths, the AWS keys and the entire config file, SparkSession and SparkContext objects