from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UploadParquet").getOrCreate()
local_path = "C:/Users/George/Desktop/JR AI engineer/JRDE16-JLA/sample.parquet"  # 替换为本地路径
dbfs_path = "dbfs:FileStore/my_parquet_files/sample.parquet"
spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).copyFromLocalFile(False, True, local_path, dbfs_path)
print(f"File uploaded to {dbfs_path}")