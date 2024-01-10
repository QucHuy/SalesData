from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector
import time
from configparser import ConfigParser

conf_file_path = "D:/Real-timeSales"
conf_file_name = conf_file_path + "/app.conf"
config_obj = ConfigParser()
config_read_obj = config_obj.read(conf_file_name)


#Kafka

kafka_host_name = config_obj.get('kafka', 'host')
port_name = config_obj.get('kafka', 'port_no')
input_kafka_topic_name = config_obj.get('kafka', 'input_topic_name')
output_kafka_topic_name = config_obj.get('kafka', 'output_topic_name')
kafka_bootstrap_servers = kafka_host_name + ':' + port_name

#mysql
mysql_host_name = config_obj.get('mysql', 'host')
mysql_port_no = config_obj.get('mysql', 'port_no')
mysql_user_name = config_obj.get('mysql', 'username')
mysql_password = config_obj.get('mysql', 'password')
mysql_db_name = config_obj.get('mysql', 'db_name')
mysql_driver = config_obj.get('mysql', 'driver')

mysql_salesbycardtype_tbl = config_obj.get('mysql', 'mysql_salesbycardtype_tbl')
mysql_salesbycountry_tbl = config_obj.get('mysql', 'mysql_salesbycountry_tbl')

mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(mysql_port_no) + "/" + mysql_db_name

db_properties = {}
db_properties['user'] = mysql_user_name
db_properties['password']= mysql_password
db_properties['driver'] = mysql_driver



def save_to_mysql_table(current_df, epoc_id , mysql_table_name):
    print("Inside save_to_mysql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mysql_table_name: " + mysql_table_name)

    mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + str(mysql_port_no) + "/" + mysql_db_name

    current_df = current_df.withColumn('batch_no', lit(epoc_id))
    current_df.write.jdbc(url = mysql_jdbc_url,
                          table = mysql_table_name,
                          mode = 'append',
                          properties = db_properties)
    # current_df.write.format("jdbc") \
    # .option("driver","com.mysql.cj.jdbc.Driver") \
    # .option("url", "jdbc:mysql://localhost:3306/ecom_db") \
    # .option("dbtable", mysql_table_name) \
    # .mode("append") \
    # .option("user", "root") \
    # .option("password", "mysql") \
    # .save()
    print("Exit")


if __name__ == "__main__":
    print("Realtime data processing application started...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
            .builder \
            .appName("Real-Time Data Processing with Kafka Source and Message Format as JSON") \
            .master("local[*]") \
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    orders_df  = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("subscribe", input_kafka_topic_name) \
                .option("startingOffsets", "latest") \
                .load()
    print("Printing Schea of order_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", 'timestamp')


    orders_schema = StructType() \
                    .add("order_id", StringType()) \
                    .add("order_product_name", StringType()) \
                    .add("order_card_type", StringType()) \
                    .add("order_amount", StringType()) \
                    .add("order_datetime", StringType()) \
                    .add("order_country_name", StringType()) \
                    .add("order_city_name", StringType()) \
                    .add("order_ecommerce_website_name", StringType())
    
    orders_df2 = orders_df1.select(from_json(col("value"), orders_schema) \
                            .alias("orders"), "timestamp")

    orders_df2.printSchema()

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    print("Printing schema of orders_df3 before")
    orders_df3.printSchema()
    
    orders_df3 = orders_df3.withColumn("partition_date", to_date("order_datetime"))
    orders_df3 = orders_df3.withColumn("partition_hour", hour(to_timestamp("order_datetime", "yyyy-MM-dd HH:mm:ss")))
    print("Printing schema of orders_df3 after")
    orders_df3.printSchema()
    orders_agg_write_stream_pre = orders_df3 \
                                    .writeStream \
                                    .trigger(processingTime = "10 seconds") \
                                    .outputMode("update") \
                                    .option("truncate", "false") \
                                    .format("console") \
                                    .start()
    
    orders_agg_write_stream_pre_hdfs = orders_df3.writeStream \
                                        .trigger(processingTime = '10 seconds') \
                                        .format('parquet') \
                                        .option("path", "/tmp/data/ecom_data/raw") \
                                        .option("checkpointLocation", "orders-agg-write-stream-pre-checkpoint") \
                                        .partitionBy("partition_date", "partition_hour") \
                                        .start()
    
    orders_df4 = orders_df3.groupBy("order_card_type") \
                            .agg({'order_amount' : 'sum'})\
                            .select("order_card_type", col("sum(order_amount)")\
                            .alias('total_sales'))
    
    print("Printing schema of orders_df4: ")
    orders_df4.printSchema()

    orders_df4 = orders_df4.withColumnRenamed("order_card_type", "card_type")
    orders_df4.printSchema()

    orders_df4.writeStream \
                .trigger(processingTime = '10 seconds') \
                .outputMode("update") \
                .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id, mysql_salesbycardtype_tbl)) \
                .start()
    
    orders_df5 = orders_df3.groupBy("order_country_name") \
                            .agg({'order_amount' : 'sum'})\
                            .select("order_country_name", col("sum(order_amount)")\
                            .alias('total_sales'))
    print("Printing schema of orders_df5: ")
    orders_df5.printSchema()

    orders_df5 = orders_df5.withColumnRenamed("order_country_name", "country_name")
    orders_df5.printSchema()

    orders_df5.writeStream \
                .trigger(processingTime = '10 seconds') \
                .outputMode("update") \
                .foreachBatch(lambda current_df, epoc_id: save_to_mysql_table(current_df, epoc_id, mysql_salesbycountry_tbl)) \
                .start()
    
    orders_agg_write_stream = orders_df4 \
                                .writeStream\
                                .trigger(processingTime = '10 seconds')\
                                .outputMode("update") \
                                .option("truncate", "false") \
                                .format("console") \
                                .start()
    
    '''
    kafka_orders_df4 = orders_df4.selectExpr("card_type as key",
                                                """to_json(named_struct('card_type', card_type
                                                                        'total_sales', total_sales)) as value""")
    #kafka_orders_df4 [key, value]
    kafka_write_query = kafka_orders_df4\
                        .writeStream\
                        .trigger(processingTime = '10 seconds')\
                        .queryName("Kafka Writer")\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "192.168.56.1:9092")\
                        .option("topic", output_kafka_topic_name)\
                        .outputMode("update")\
                        .option("checkpointLocation", "kafka-check-point-dir")\
                        .start()
    '''

    orders_agg_write_stream.awaitTermination()


    print("Complete")
    