// Databricks notebook source
// MAGIC %md # Connecting to Redshift from Databricks.
// MAGIC 
// MAGIC This notebook contains examples showing how to use Databricks' [spark-redshift](https://github.com/databricks/spark-redshift) library for bulk reading/writing of large datasets.
// MAGIC 
// MAGIC For more moderate-sized tables in Redshift, we recommend using a postgres driver.
// MAGIC 
// MAGIC `spark-redshift` is a library to load data into Spark SQL DataFrames from Amazon Redshift, and write data back to Redshift tables. Amazon S3 is used to efficiently transfer data in and out of Redshift, and JDBC is used to automatically trigger the appropriate COPY and UNLOAD commands on Redshift.
// MAGIC 
// MAGIC This library is more suited to ETL than interactive queries, since large amounts of data could be extracted to S3 for each query execution. If you plan to perform many queries against the same Redshift tables then we recommend saving the extracted data in a format such as Parquet.

// COMMAND ----------

// MAGIC %md ## Configuration
// MAGIC 
// MAGIC There are three main configuration steps which are prerequisites to using `spark-redshift`:
// MAGIC 
// MAGIC - Create a S3 bucket to hold temporary data.
// MAGIC - Configure AWS credentials to access that S3 bucket.
// MAGIC - Configure Redshift access credentials.

// COMMAND ----------

// MAGIC %md ### Configuring a S3 bucket to hold temporary files 
// MAGIC 
// MAGIC `spark-redshift` reads and writes data to S3 when transferring data to/from Redshift, so you'll need to specify a path in S3 where the library should write these temporary files. `spark-redshift` cannot automatically clean up the temporary files that it creates in S3. As a result, we recommend that you use a dedicated temporary S3 bucket with an [object lifecycle configuration](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html) to ensure that temporary files are automatically deleted after a specified expiration period.
// MAGIC 
// MAGIC In this tutorial, we'll use a bucket named `spark-redshift-testing` to hold our temporary directory:
// MAGIC 
// MAGIC You must choose an S3 bucket for the temp folder that is in the same region as your Databricks Cluster.

// COMMAND ----------

// MAGIC %scala
// MAGIC val tempDir = "s3n://spark-redshift-testing/temp/"

// COMMAND ----------

// MAGIC %md ### Configuring AWS credentials to access the S3 bucket
// MAGIC 
// MAGIC Next, we'll have to configure AWS access credentials so that `spark-redshift` is able to write temporary files to this S3 bucket. `spark-redshift` supports [multiple methods](https://github.com/databricks/spark-redshift#aws-credentials) for configuring AWS credentials, but in this tutorial we'll simply set the proper configuration properties on `SparkContext`'s global `hadoopConfiguration`:

// COMMAND ----------

// MAGIC %scala
// MAGIC // Set your s3a and/or s3n keys depending on the temp directory set above
// MAGIC sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_ACCESS_KEY")
// MAGIC sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_KEY")
// MAGIC 
// MAGIC sc.hadoopConfiguration.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
// MAGIC sc.hadoopConfiguration.set("fs.s3a.secret.key", "YOUR_SECRET_KEY") 

// COMMAND ----------

// MAGIC %md ### Configuring Redshift database credentials
// MAGIC 
// MAGIC Finally, you'll need to provide a JDBC URL for accessing your Redshift cluster. You can obtain your database's hostname from the AWS web UI. You'll also have to encode a database username and password into the URL. `spark-redshift`'s [full configuration guide](https://github.com/databricks/spark-redshift#parameters) contains additional information on how to specify this URL.

// COMMAND ----------

// MAGIC %scala
// MAGIC val jdbcUsername = "REPLACE_WITH_YOUR_USER"
// MAGIC val jdbcPassword = "REPLACE_WITH_YOUR_PASSWORD"
// MAGIC val jdbcHostname = "REPLACE_WITH_YOUR_REDSHIFT_HOST"
// MAGIC val jdbcPort = 5439
// MAGIC val jdbcDatabase = "REPLACE_WITH_DATABASE"
// MAGIC val jdbcUrl = s"jdbc:redshift://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

// COMMAND ----------

// MAGIC %md ## Writing data to Redshift
// MAGIC 
// MAGIC In this section, we'll show how to write data back to Redshift. In the next section, on reading data, we'll load these tables from Redshift back into DataFrames.
// MAGIC 
// MAGIC We'll use `diamonds`, a sample SQL table that already exists in this cluster:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM diamonds LIMIT 5

// COMMAND ----------

// MAGIC   %md Let's save this into a Redshift table, which we'll also name `diamonds` (the name of the table in Redshift could be different, though):

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.sql("select * from diamonds")
// MAGIC   .write
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl) // <--- JDBC URL that we configured earlier
// MAGIC   .option("tempdir", tempDir) // <--- temporary bucket that we created earlier
// MAGIC   .option("dbtable", "diamonds") // <--- name of the table to create in Redshift
// MAGIC   .save()

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC `spark-redshift` automatically creates a Redshift table with the appropriate schema determined from the table/DataFrame being written.
// MAGIC 
// MAGIC The default behavior is to create a new table and to throw an error message if a table with the same name already exists. 
// MAGIC You can use Spark SQL's `SaveMode` feature to change this behavior. For example, let's append more rows to our table:

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.SaveMode
// MAGIC spark.sql("select * from diamonds limit 10")
// MAGIC   .write
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl)
// MAGIC   .option("tempdir", tempDir)
// MAGIC   .option("dbtable", "diamonds")
// MAGIC   .mode(SaveMode.Append) // <--- Append to the existing table
// MAGIC   .save()

// COMMAND ----------

// MAGIC %md You can also overwrite the existing table:

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.sql("select * from diamonds")
// MAGIC   .write
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl)
// MAGIC   .option("tempdir", tempDir)
// MAGIC   .option("dbtable", "diamonds")
// MAGIC   .mode(SaveMode.Overwrite) // <--- Drop the existing table, then write the new data
// MAGIC   .save()

// COMMAND ----------

// MAGIC %md The examples above showed the Scala API, but `spark-redshift` is also usable from SQL, Python, R, and Java.
// MAGIC 
// MAGIC Here's an example of reading a Redshift table from SQL:
// MAGIC 
// MAGIC > Note that the SQL API only supports the creation of new tables and not overwriting or appending; this corresponds to the default save mode of the other language APIs.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE diamonds_temp_v0 -- <--- cannot use same name, since this also registers a table in Spark
// MAGIC USING com.databricks.spark.redshift
// MAGIC OPTIONS (
// MAGIC   dbtable 'diamonds', -- Since we can't specify ovewrite modes in the SQL API
// MAGIC   tempdir 's3n://spark-redshift-testing/temp/',
// MAGIC   url 'jdbc:redshift://your_host_name.us-west-2.redshift.amazonaws.com:5439/tablename?user=user&password=password')

// COMMAND ----------

// MAGIC %md You can use `CREATE TABLE AS` syntax to create a table in Redshift from a Spark SQL table.
// MAGIC 
// MAGIC The following example copies data from a Spark SQL table (`diamonds`) to new Redshift table (`diamonds_copy_v0`), then creates a Spark SQL table (`diamonds_temp_v01`) to reference the new Redshift table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE diamonds_temp_v01 -- <--- cannot use same name, since this also registers a table in Spark
// MAGIC USING com.databricks.spark.redshift
// MAGIC OPTIONS (
// MAGIC   dbtable 'diamonds_copy_v0', -- Since we can't specify ovewrite modes in the SQL API
// MAGIC   tempdir 's3n://spark-redshift-testing/temp/',
// MAGIC   url 'jdbc:redshift://your_host_name.us-west-2.redshift.amazonaws.com:5439/tablename?user=user&password=password')
// MAGIC AS SELECT
// MAGIC  carat, cut, color, clarity, depth, `table`, price, x, y, z
// MAGIC FROM diamonds

// COMMAND ----------

// MAGIC %md ## Reading data from Redshift
// MAGIC 
// MAGIC In this section, we'll load data from the Redshift tables that we created in the previous section. In this first example, we'll create a DataFrame from an entire Redshift table:

// COMMAND ----------

// MAGIC %scala
// MAGIC val diamonds_from_redshift = spark.read
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl) // <--- JDBC URL that we configured earlier
// MAGIC   .option("tempdir", tempDir) // <--- temporary bucket that we created earlier
// MAGIC   .option("dbtable", "diamonds") // <--- name of the table in Redshift
// MAGIC   .load()

// COMMAND ----------

// MAGIC %scala
// MAGIC diamonds_from_redshift.registerTempTable("tmp_diamonds")

// COMMAND ----------

// MAGIC %sql select * from tmp_diamonds

// COMMAND ----------

// MAGIC %md As you can see, `spark-redshift` automatically reads the schema from the Redshift table and maps its types back to Spark SQL's types:

// COMMAND ----------

// MAGIC %scala
// MAGIC diamonds_from_redshift.printSchema()

// COMMAND ----------

// MAGIC %md We can run queries against this table:

// COMMAND ----------

// MAGIC %scala
// MAGIC display(diamonds_from_redshift.select("carat", "cut", "price").groupBy("carat", "cut").avg("price"))

// COMMAND ----------

// MAGIC %md You can also load tables using the SQL API:

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC CREATE TEMPORARY TABLE diamonds_from_redshift
// MAGIC USING com.databricks.spark.redshift
// MAGIC OPTIONS (
// MAGIC   dbtable 'diamonds',
// MAGIC   tempdir 's3n://spark-redshift-testing/temp/',
// MAGIC   url 'jdbc:redshift://your_host_name.us-west-2.redshift.amazonaws.com:5439/tablename?user=user&password=password');
// MAGIC 
// MAGIC SELECT * FROM diamonds_from_redshift limit 5;

// COMMAND ----------

// MAGIC %md ### Creating DataFrames from the results of Redshift queries
// MAGIC 
// MAGIC In addition to loading entire tables, `spark-redshift` can create DataFrames from the results of Redshift queries. To do this, specify the `query` parameter in place of `dbtable`. In the following example, we translate the above query into a Redshift query then create a DataFrame from that query's result. This pushes more of the processing into Redshift itself:

// COMMAND ----------

// MAGIC %scala
// MAGIC val fromRedshiftQuery = spark.read
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl)
// MAGIC   .option("tempdir", tempDir)
// MAGIC   .option("query", "SELECT carat, cut, AVG(price) as avg_price from diamonds group by carat, cut")
// MAGIC   .load()

// COMMAND ----------

// MAGIC %scala
// MAGIC display(fromRedshiftQuery)

// COMMAND ----------

// MAGIC %md ## Additional Configuration Options
// MAGIC 
// MAGIC ###Configuring the maximum size of string columns
// MAGIC 
// MAGIC When creating Redshift tables, spark-redshift's default behavior is to create `TEXT` columns for string columns. Redshift stores `TEXT` columns as `VARCHAR(256)`, so these columns have a maximum size of 256 characters ([source](http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html)).
// MAGIC 
// MAGIC To support larger columns, you can use the `maxlength` column metadata field to specify the maximum length of individual string columns. This can also be done as a space-savings performance optimization in order to declare columns with a smaller maximum length than the default.
// MAGIC 
// MAGIC > Note: Column metadata modification is unsupported in the Python, SQL, and R language APIs.
// MAGIC 
// MAGIC Here is an example of updating multiple columns' metadata fields using Spark's Scala API:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT max(length(cut)) AS cut_size, max(length(color)) AS color_size, max(length(clarity)) AS clarity_size FROM diamonds

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.MetadataBuilder
// MAGIC 
// MAGIC // Specify the custom width of each column
// MAGIC val columnLengthMap = Map(
// MAGIC   "cut" -> 9,
// MAGIC   "color" -> 1,
// MAGIC   "clarity" -> 4
// MAGIC )
// MAGIC 
// MAGIC var diamondsDf = spark.sql("select * from diamonds").withColumnRenamed("table", "_table")
// MAGIC 
// MAGIC // Apply each column metadata customization
// MAGIC columnLengthMap.foreach { case (colName, length) =>
// MAGIC   val metadata = new MetadataBuilder().putLong("maxlength", length).build()
// MAGIC   diamondsDf = diamondsDf.withColumn(colName, diamondsDf(colName).as(colName, metadata))
// MAGIC }
// MAGIC 
// MAGIC diamondsDf.write
// MAGIC   .format("com.databricks.spark.redshift")
// MAGIC   .option("url", jdbcUrl)
// MAGIC   .option("tempdir", tempDir)
// MAGIC   .option("dbtable", "diamonds_optimized")
// MAGIC   .save()

// COMMAND ----------

// MAGIC %md ## Notes on spark-redshift internals
// MAGIC 
// MAGIC ### Databricks Pre-loads Libraries Automatically
// MAGIC Databricks preloads the necessary libraries to use `spark-redshift` so you don't have to import and attach them yourself.
// MAGIC 
// MAGIC - [Redshift Connector JAR](https://github.com/databricks/spark-redshift)
// MAGIC - [AWS Redshift SDK](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-redshift)

// COMMAND ----------

// MAGIC %md ###Reading from Redshift
// MAGIC `spark-redshift` executes a Redshift [UNLOAD](http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command (using JDBC) which copies the Redshift table in parallel to a temporary S3 bucket provided by the user. Next it reads these S3 files in parallel using the Hadoop InputFormat API and maps it to an RDD instance. Finally, it applies the schema of the table (or query), retrieved using JDBC metadata retrieval capabilities, to the RDD generated in the prior step to create a DataFrame instance.
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2015/10/image01.gif "Redshift Data Ingestion")

// COMMAND ----------

// MAGIC %md ### Writing to Redshift
// MAGIC `spark-redshift` will first create the table in Redshift using JDBC. It then copies the partitioned RDD encapsulated by the source DataFrame instance to the temporary S3 folder. Finally, it executes the Redshift [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command that performs a high performance distributed copy of S3 folder contents to the newly created Redshift table.
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2015/10/image00.gif "Redshift Data Egression")
