import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- 1. SETUP ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Simulation inputs
input_transaction_date = "20260123"
input_run_index = "1"

# --- 2. CREATE DUMMY SOURCE DATA ---
# This represents your original data containing 'transactiondate'
data = [
    (1, "Product A", 100, 20260123),
    (2, "Product B", 200, 20260123)
]
columns = ["id", "product", "amount", "transactiondate"]
df = spark.createDataFrame(data, columns)

# --- 3. TRANSFORMATION (Create the new Partition Key) ---
# We keep the original 'transactiondate' untouched.
# We create a NEW column 'transactiondate_index' just for the folder name.

df_final = df.withColumn(
    "transactiondate_index",
    F.format_string(
        "%s_%s", 
        F.date_format(
            F.to_date(F.col("transactiondate").cast(StringType()), "yyyyMMdd"), 
            "yyyy-MM-dd"
        ), 
        F.lit(input_run_index)
    )
)

print("--- Preview Data before Write ---")
# You will see BOTH columns here
df_final.show()

# --- 4. WRITE TO S3 (The Critical Step) ---
s3_output_path = "s3://your-target-bucket/processed-data/"

# systematic thinking: 
# We partition ONLY by 'transactiondate_index'.
# Therefore, 'transactiondate' is treated as regular data and written into the file.

df_final.write \
    .mode("append") \
    .partitionBy("transactiondate_index") \
    .parquet(s3_output_path)

print(f"Written to S3 partitioned by transactiondate_index.")

# --- 5. VERIFICATION (Proof) ---
# If we read the specific parquet file back (bypassing the folder structure),
# we can see what physically exists inside the file.

# We verify by reading the underlying parquet files directly
# (This part is just for you to verify, you don't need it in production)
print("--- Verifying File Content (Reading back from S3) ---")
# verify_df = spark.read.parquet(s3_output_path)
# verify_df.show()
