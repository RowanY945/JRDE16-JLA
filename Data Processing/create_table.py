from pyspark.sql import SparkSession

# 创建 SparkSession（通过 databricks-connect 连接到 Databricks）
spark = SparkSession.bdatabricks configure --tokenuilder.appName("ParquetToTable").getOrCreate()

# 读取 Parquet 文件（替换为上传后的路径）
parquet_path = "C:/Users/George/Desktop/JR AI engineer/JRDE16-JLA/sample.parquet"
df = spark.read.parquet(parquet_path)

# 写入 Delta 表（替换为你的数据库和表名）
target_table = "my_dbt_database.sample_table"
df.write.format("delta").mode("overwrite").saveAsTable(target_table)
