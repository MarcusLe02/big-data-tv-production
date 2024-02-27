from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date as pyspark_to_date
import os
import datetime

# Initialize Spark session
spark = SparkSession.builder.config("spark.driver.memory", "15g") \
    .config("spark.executor.cores", 16) \
    .config('spark.driver.extraClassPath', 
            '/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/jars/mysql-connector-j-8.2.0.jar').getOrCreate()

def convert_to_datevalue(value):
    return datetime.datetime.strptime(value, "%Y%m%d").date()

def date_range(start_date, end_date):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d") + ".json")
        current_date += datetime.timedelta(days=1)
    return date_list

def generate_date_range(from_date, to_date):
    from_date = convert_to_datevalue(from_date)
    to_date = convert_to_datevalue(to_date)
    date_list = date_range(from_date, to_date)
    return date_list

def main_task(path, save_path, from_date, to_date):
    dates = generate_date_range(from_date, to_date)
    first_file_path = os.path.join(path, dates[0])
    log_df = spark.read.json(first_file_path).withColumn("Date", lit(dates[0]))

    for log in dates[1:]:
        file_path = os.path.join(path, log)
        df = spark.read.json(file_path)
        df = df.withColumn("Date", lit(log))
        log_df = log_df.union(df)

    print('---------------------')
    print('Showing data structure:')
    log_df.printSchema()
    print('---------------------')
    print('Number of records:')
    print(log_df.count())
    log_df.show()
    print('---------------------')
    print('Transforming data...')
    log_df = log_df.withColumn("Date", pyspark_to_date(substring(log_df["Date"], 0, 8), "yyyyMMdd")) \
                   .withColumnRenamed('_id', 'ID') \
                   .withColumnRenamed('_index', 'Index') \
                   .withColumnRenamed('_score', 'Score') \
                   .withColumnRenamed('_type', 'Type') \
                   .select('ID', 'Index', 'Score', 'Type', '_source.Contract', '_source.AppName', '_source.Mac', '_source.TotalDuration', 'Date') \
                   .withColumn("RelaxDuration", expr("CASE WHEN AppName = 'RELAX' THEN TotalDuration ELSE 0 END")) \
                   .withColumn("ChildDuration", expr("CASE WHEN AppName = 'CHILD' THEN TotalDuration ELSE 0 END")) \
                   .withColumn("MovieDuration", expr("CASE WHEN AppName IN ('FIMS', 'FIMS_RES', 'VOD', 'VOD_RES', 'BHD', 'BHD_RES', 'DANET') THEN TotalDuration ELSE 0 END")) \
                   .withColumn("TVDuration", expr("CASE WHEN AppName IN ('CHANNEL', 'KPLUS', 'APP', 'DSHD') THEN TotalDuration ELSE 0 END")) \
                   .withColumn("SportDuration", expr("CASE WHEN AppName = 'SPORT' THEN TotalDuration ELSE 0 END"))

    print('---------------------')
    print('Pivoting data...')
    # Group by Contract & Date
    grouped_df = log_df.groupBy("Contract", "Date").agg(
    sum("TVDuration").alias("TVDuration"),
    sum("MovieDuration").alias("MovieDuration"),
    sum("RelaxDuration").alias("RelaxDuration"),
    sum("SportDuration").alias("SportDuration"),
    sum("ChildDuration").alias("ChildDuration"))
    grouped_df.show()

    duration_columns = ['TVDuration', 'MovieDuration', 'RelaxDuration', 'SportDuration', 'ChildDuration']

    # Determine activeness, group by Contract
    activeness_expr = (
    when((col("TVDuration") > 0) |
         (col("MovieDuration") > 0) |
         (col("RelaxDuration") > 0) |
         (col("SportDuration") > 0) |
         (col("ChildDuration") > 0), 1).otherwise(0))
    
    grouped_df = grouped_df.withColumn("Activeness", activeness_expr)
    agg_df = grouped_df.groupBy('Contract').agg(*[sum(col(col_name)).alias(col_name) for col_name in duration_columns], (sum("Activeness")/30).alias("Activeness"))
    agg_df.show()

    # Determine most watched category
    max_value = greatest(*[col(col_name) for col_name in duration_columns])
    most_watched_expr = (
        when(max_value == col("TVDuration"), "TV") \
        .when(max_value == col("MovieDuration"), "Movie") \
        .when(max_value == col("RelaxDuration"), "Relax") \
        .when(max_value == col("SportDuration"), "Sport") \
        .when(max_value == col("ChildDuration"), "Child"))

    agg_df = agg_df.withColumn("MostWatched", most_watched_expr)
    agg_df.show()

    # Create a formatted string for customer taste
    customer_taste_expr = concat_ws("-", 
    when(col("ChildDuration") > 0, "Child"),
    when(col("MovieDuration") > 0, "Movie"),
    when(col("RelaxDuration") > 0, "Relax"),
    when(col("SportDuration") > 0, "Sport"),
    when(col("TVDuration") > 0, "TV"))

    agg_df = agg_df.withColumn("Taste", customer_taste_expr)

    print('---------------------')
    print('Showing result output:')
    # Show the resulting DataFrame
    agg_df.show()
    print('---------------------')
    print('Saving result output...')
    print('-----------------------------')

    save_path = save_path + f"/{from_date}_{to_date}"
    agg_df.repartition(1).write.csv(save_path, header=True, mode='overwrite')

    print('Output saved to local file system.')
    print('-----------------------------')
    print('Connecting to MySQL')
    print('-----------------------------')

    url = "jdbc:mysql://localhost/log_db"
    connection_properties = {
    "user": "root",
    "password": "5nam",
    "driver": "com.mysql.cj.jdbc.Driver"
    }
    dbtable = "log_summary"
    agg_df.write.jdbc(url=url, table=dbtable, mode='overwrite', properties=connection_properties)

    print('Output saved to MySQL.')

main_task("/Users/marcusle02/Documents/Learning/study_de/BD/data/log_content", "/Users/marcusle02/Documents/Learning/study_de/BD/data/log_content", "20220401", "20220430")

# For creating table on MySQL
"""
CREATE TABLE log_summary (
    Contract VARCHAR(255) PRIMARY KEY,
    TVDuration INTEGER,
    MovieDuration INTEGER,
    RelaxDuration INTEGER,
    SportDuration INTEGER,
    ChildDuration INTEGER,
    Activeness DOUBLE,
    MostWatched VARCHAR(255),
    Taste VARCHAR(255)
);
"""