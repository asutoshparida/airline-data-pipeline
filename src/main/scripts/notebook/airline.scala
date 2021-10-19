// Databricks notebook source
// MAGIC %md # Load CSV data from FileStore

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{expr, map_keys}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, trim}

val airline_location = "/FileStore/tables/airlines-4.csv"
val airport_location = "/FileStore/tables/airports-2.csv"
val flights_location = "/FileStore/tables/data.csv"

    val airportSchema = StructType(Array(
      StructField("IATA_CODE", StringType, true),
      StructField("AIRPORT", StringType, true),
      StructField("CITY", StringType, true),
      StructField("STATE", StringType, true),
      StructField("COUNTRY", StringType, true),
      StructField("LATITUDE", StringType, true),
      StructField("LONGITUDE", StringType, true))
    )

val airlineSchema = StructType(Array(
      StructField("IATA_CODE", StringType, true),
      StructField("AIRLINE", StringType, true))
    )

val flightSchema = StructType(Array(
      StructField("YEAR", IntegerType, true),
      StructField("MONTH", IntegerType, true),
      StructField("DAY", IntegerType, true),
      StructField("DAY_OF_WEEK", IntegerType, true),
      StructField("AIRLINE", StringType, true),
      StructField("FLIGHT_NUMBER", IntegerType, true),
      StructField("TAIL_NUMBER", StringType, true),
      StructField("ORIGIN_AIRPORT", StringType, true),
      StructField("DESTINATION_AIRPORT", StringType, true),
      StructField("SCHEDULED_DEPARTURE", IntegerType, true),
      StructField("DEPARTURE_TIME", IntegerType, true),
      StructField("DEPARTURE_DELAY", IntegerType, true),
      StructField("TAXI_OUT", IntegerType, true),
      StructField("WHEELS_OFF", IntegerType, true),
      StructField("SCHEDULED_TIME", IntegerType, true),
      StructField("ELAPSED_TIME", IntegerType, true),
      StructField("AIR_TIME", IntegerType, true),
      StructField("DISTANCE", IntegerType, true),
      StructField("WHEELS_ON", IntegerType, true),
      StructField("TAXI_IN", IntegerType, true),
      StructField("SCHEDULED_ARRIVAL", IntegerType, true),
      StructField("ARRIVAL_TIME", IntegerType, true),
      StructField("ARRIVAL_DELAY", IntegerType, true),
      StructField("DIVERTED", IntegerType, true),
      StructField("CANCELLED", IntegerType, true),
      StructField("CANCELLATION_REASON", StringType, true),
      StructField("AIR_SYSTEM_DELAY", IntegerType, true),
      StructField("SECURITY_DELAY", IntegerType, true),
      StructField("AIRLINE_DELAY", IntegerType, true),
      StructField("LATE_AIRCRAFT_DELAY", IntegerType, true),
      StructField("WEATHER_DELAY", IntegerType, true))
    )

 val airportDataFrame: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(airportSchema)
      .load(airport_location)


val airlineDataFrame: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(airlineSchema)
      .load(airline_location)


val stageFlightsDF: DataFrame = spark.sqlContext.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(flightSchema)
      .load(flights_location)

// COMMAND ----------

// MAGIC %md # Perform data cleanup & join all data sets

// COMMAND ----------

val flightsDataFrame = stageFlightsDF.withColumn("EVENT_DATE", expr("make_date(YEAR, MONTH, DAY)"))

import spark.implicits._
val firstStageDataFrame = flightsDataFrame.join(airlineDataFrame, flightsDataFrame("AIRLINE") ===  airlineDataFrame("IATA_CODE"))
      .select(flightsDataFrame("*"), trim(airlineDataFrame.col("AIRLINE")).alias("AIRLINE_NAME"),($"ARRIVAL_DELAY" +        $"DEPARTURE_DELAY").alias("TOTAL_DELAY")).drop("AIRLINE")

    val secondStageDataFrame = firstStageDataFrame.join(airportDataFrame, firstStageDataFrame("ORIGIN_AIRPORT") ===  airportDataFrame("IATA_CODE"))
      .select(firstStageDataFrame("*"), trim(airportDataFrame.col("AIRPORT")).alias("ORIGIN_AIRPORT_NAME"),
        airportDataFrame.col("CITY").alias("ORIGIN_AIRPORT_CITY"),
        airportDataFrame.col("STATE").alias("ORIGIN_AIRPORT_STATE"),
        airportDataFrame.col("COUNTRY").alias("ORIGIN_AIRPORT_COUNTRY"),
        airportDataFrame.col("LATITUDE").alias("ORIGIN_AIRPORT_LATITUDE"),
        airportDataFrame.col("LONGITUDE").alias("ORIGIN_AIRPORT_LONGITUDE")
      ).drop("IATA_CODE")


    val finalAirlinesDataFrame = secondStageDataFrame.join(airportDataFrame, secondStageDataFrame("DESTINATION_AIRPORT") ===  airportDataFrame("IATA_CODE"))
      .select(secondStageDataFrame("*"), trim(airportDataFrame.col("AIRPORT")).alias("DESTINATION_AIRPORT_NAME"),
        airportDataFrame.col("CITY").alias("DESTINATION_AIRPORT_CITY"),
        airportDataFrame.col("STATE").alias("DESTINATION_AIRPORT_STATE"),
        airportDataFrame.col("COUNTRY").alias("DESTINATION_AIRPORT_COUNTRY"),
        airportDataFrame.col("LATITUDE").alias("DESTINATION_AIRPORT_LATITUDE"),
        airportDataFrame.col("LONGITUDE").alias("DESTINATION_AIRPORT_LONGITUDE")
      ).drop("IATA_CODE")

    finalAirlinesDataFrame.select("DEPARTURE_DELAY", "ARRIVAL_DELAY","TOTAL_DELAY").show()
    finalAirlinesDataFrame.printSchema()
//     finalAirlinesDataFrame.explain()

// COMMAND ----------

// MAGIC %md # Load final curated data to Snowflake

// COMMAND ----------

val sfOptions: Map[String, String] = Map(
      "sfURL" -> "https://lua28621.us-east-1.snowflakecomputing.com/",
      "sfAccount" -> "lua28621",
      "sfUser" -> "",
      "sfPassword" -> "",
      "sfDatabase" -> "USER_ASUTOSH",
      "sfSchema" -> "CURATED",
      "sfRole" -> "SYSADMIN")

    finalAirlinesDataFrame.write
      .format("snowflake")
      .options(sfOptions)
      .option("dbtable", "AIRLINE_AGGREGATED")
      .mode(SaveMode.Append)
      .save()

// COMMAND ----------

// MAGIC %md # Load final curated data from Snowflake

// COMMAND ----------

val sfOptions: Map[String, String] = Map(
      "sfURL" -> "https://lua28621.us-east-1.snowflakecomputing.com/",
      "sfAccount" -> "lua28621",
      "sfUser" -> "",
      "sfPassword" -> "",
      "sfDatabase" -> "USER_ASUTOSH",
      "sfSchema" -> "CURATED",
      "sfRole" -> "SYSADMIN")

val task1Query = "select * from CURATED.task1_view"
val task2Query = "select * from CURATED.task2_view"
val task3Query = "select * from CURATED.task3_view"
val task4Query = "select * from CURATED.task4_view"
val task6Query = "select * from CURATED.task6_view"

val task1DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", task1Query)
      .load()

val task2DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", task2Query)
      .load()

val task3DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", task3Query)
      .load()

val task4DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", task4Query)
      .load()

val task6DataFrame = spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", task6Query)
      .load()


// COMMAND ----------

// MAGIC %md # Reports

// COMMAND ----------

// MAGIC %md ### Total number of flights by airline and airport on a monthly basis

// COMMAND ----------

display(task1DataFrame)

// COMMAND ----------

// MAGIC %md ###On-time percentage of each airline for the year 2015

// COMMAND ----------

display(task2DataFrame)

// COMMAND ----------

// MAGIC %md ###Airline with the most unique routes

// COMMAND ----------

display(task6DataFrame)

// COMMAND ----------

// MAGIC %md ### Cancellation reasons by airport

// COMMAND ----------

display(task4DataFrame)

// COMMAND ----------

// MAGIC %md ###Airlines with the largest number of delays

// COMMAND ----------

display(task3DataFrame)
