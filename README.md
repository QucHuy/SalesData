- First, declare variables in appconf file 
- Second, run the kafkaproducer.py to generate faking data
- Third, cd to your code file and run the command below
  
spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:5.1.49 --files #appconf-file-path #processing-file-path
