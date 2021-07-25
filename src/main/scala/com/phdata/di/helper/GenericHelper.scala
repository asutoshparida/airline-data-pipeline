package com.phdata.di.helper

import com.phdata.di.utility.GenericUtility
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, expr, lit, map_keys, trim}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.typesafe.scalalogging.LazyLogging

class GenericHelper(genericUtility: GenericUtility) extends LazyLogging {

  /***
   * Prepare Airports Data From File Store
   * @param dataFileLocation
   * @param sparkSession
   * @throws java.lang.Exception
   * @return
   */
  @throws(classOf[Exception])
  def prepareAirportData(dataFileLocation:String, sparkSession: SparkSession): DataFrame = {
    var airportDF: DataFrame = null;

    val airportSchema = StructType(Array(
      StructField("IATA_CODE", StringType, true),
      StructField("AIRPORT", StringType, true),
      StructField("CITY", StringType, true),
      StructField("STATE", StringType, true),
      StructField("COUNTRY", StringType, true),
      StructField("LATITUDE", StringType, true),
      StructField("LONGITUDE", StringType, true))
    )

    try {
      airportDF = genericUtility.prepareDataFrameFromFile(dataFileLocation, ",", "", sparkSession.sqlContext, airportSchema)
    } catch {
      case exp: Exception => {
        val emptyRDD = sparkSession.sparkContext.emptyRDD[Row]
        airportDF = sparkSession.sqlContext.createDataFrame(emptyRDD, airportSchema)
        logger.error(s"GenericHelper# prepareAirportData #Failed while processing Airport Data. Exp.: ${exp.getMessage}")
      }
    }
    airportDF
  }

  /**
   * Prepare Airline Data From File Store
   * @param dataFileLocation
   * @param sparkSession
   * @throws java.lang.Exception
   * @return
   */
  @throws(classOf[Exception])
  def prepareAirlineData(dataFileLocation:String, sparkSession: SparkSession): DataFrame = {
    var airlineDF: DataFrame = null;

    val airlineSchema = StructType(Array(
      StructField("IATA_CODE", StringType, true),
      StructField("AIRLINE", StringType, true))
    )

    try {
      airlineDF = genericUtility.prepareDataFrameFromFile(dataFileLocation, ",", "", sparkSession.sqlContext, airlineSchema)
    } catch {
      case exp: Exception => {
        val emptyRDD = sparkSession.sparkContext.emptyRDD[Row]
        airlineDF = sparkSession.sqlContext.createDataFrame(emptyRDD, airlineSchema)
        logger.error(s"GenericHelper# prepareAirlineData #Failed while processing Airline data. Exp.: ${exp.getMessage}")
      }
    }
    airlineDF
  }

  /**
   * Prepare Flight Data From File Store
   * @param dataFileLocation
   * @param sparkSession
   * @throws java.lang.Exception
   * @return
   */
  @throws(classOf[Exception])
  def prepareFlightData(dataFileLocation:String, sparkSession: SparkSession): DataFrame = {
    var flightsDF: DataFrame = null;

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

    try {
      val stage_flightsDF = genericUtility.prepareDataFrameFromFile(dataFileLocation, ",", "", sparkSession.sqlContext, flightSchema)
      flightsDF = stage_flightsDF.withColumn("EVENT_DATE", expr("make_date(YEAR, MONTH, DAY)"))
    } catch {
      case exp: Exception => {
        val emptyRDD = sparkSession.sparkContext.emptyRDD[Row]
        flightsDF = sparkSession.sqlContext.createDataFrame(emptyRDD, flightSchema)
        logger.error(s"GenericHelper# prepareFlightData # Failed while processing Flights data. Exp.: ${exp.getMessage}")
      }
    }
    flightsDF
  }

  /**
   * Prepare flights data according to full load / delta load
   * @param airlineDataFrame
   * @param airportDataFrame
   * @param stageFlightsDF
   * @param lastEventDate
   * @param sparkSession
   * @throws java.lang.Exception
   * @return finalAirlinesDataFrame
   */
  @throws(classOf[Exception])
  def prepareAirlineIncrementalData(airlineDataFrame: DataFrame, airportDataFrame: DataFrame, stageFlightsDF: DataFrame, lastEventDate: String, sparkSession: SparkSession) = {
    var flightsDataFrame: DataFrame = null
    var finalAirlinesDataFrame: DataFrame = null
    try {

      if(lastEventDate == null){
          logger.info(s"GenericHelper# prepareAirlineIncrementalData # Full load :")
        flightsDataFrame = stageFlightsDF.withColumn("EVENT_DATE", expr("make_date(YEAR, MONTH, DAY)"))
      } else {
        logger.info(s"GenericHelper# prepareAirlineIncrementalData # Delta load with event_date :"+ lastEventDate )
        val tempFlightsDataFrame = stageFlightsDF.withColumn("EVENT_DATE", expr("make_date(YEAR, MONTH, DAY)"))
        flightsDataFrame = tempFlightsDataFrame.filter(tempFlightsDataFrame("EVENT_DATE").gt(lit(lastEventDate)))
      }
      logger.info(s"GenericHelper# prepareAirlineIncrementalData #Perform data cleanup & join all data sets")
      import sparkSession.implicits._
      val firstStageDataFrame = flightsDataFrame.join(airlineDataFrame, flightsDataFrame("AIRLINE") === airlineDataFrame("IATA_CODE"))
        .select(flightsDataFrame("*"), trim(airlineDataFrame.col("AIRLINE")).alias("AIRLINE_NAME"), ($"ARRIVAL_DELAY" + $"DEPARTURE_DELAY").alias("TOTAL_DELAY")).drop("AIRLINE")

      val secondStageDataFrame = firstStageDataFrame.join(airportDataFrame, firstStageDataFrame("ORIGIN_AIRPORT") === airportDataFrame("IATA_CODE"))
        .select(firstStageDataFrame("*"), trim(airportDataFrame.col("AIRPORT")).alias("ORIGIN_AIRPORT_NAME"),
          airportDataFrame.col("CITY").alias("ORIGIN_AIRPORT_CITY"),
          airportDataFrame.col("STATE").alias("ORIGIN_AIRPORT_STATE"),
          airportDataFrame.col("COUNTRY").alias("ORIGIN_AIRPORT_COUNTRY"),
          airportDataFrame.col("LATITUDE").alias("ORIGIN_AIRPORT_LATITUDE"),
          airportDataFrame.col("LONGITUDE").alias("ORIGIN_AIRPORT_LONGITUDE")
        ).drop("IATA_CODE")


      finalAirlinesDataFrame = secondStageDataFrame.join(airportDataFrame, secondStageDataFrame("DESTINATION_AIRPORT") === airportDataFrame("IATA_CODE"))
        .select(secondStageDataFrame("*"), trim(airportDataFrame.col("AIRPORT")).alias("DESTINATION_AIRPORT_NAME"),
          airportDataFrame.col("CITY").alias("DESTINATION_AIRPORT_CITY"),
          airportDataFrame.col("STATE").alias("DESTINATION_AIRPORT_STATE"),
          airportDataFrame.col("COUNTRY").alias("DESTINATION_AIRPORT_COUNTRY"),
          airportDataFrame.col("LATITUDE").alias("DESTINATION_AIRPORT_LATITUDE"),
          airportDataFrame.col("LONGITUDE").alias("DESTINATION_AIRPORT_LONGITUDE")
        ).drop("IATA_CODE")

      //    finalAirlinesDataFrame.show()
      logger.info(s"GenericHelper# prepareAirlineIncrementalData #Final curated dataFrame schema")
      finalAirlinesDataFrame.printSchema()
      //            finalAirlinesDataFrame.explain()
    } catch {
      case exp: Exception => {
        logger.error(s"GenericHelper# prepareAirlineIncrementalData # Failed while processing Airline data. Exp.: ${exp.getMessage}")
        throw exp
      }
    }
    finalAirlinesDataFrame
  }



}
