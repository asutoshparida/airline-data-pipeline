package com.phdata.di.test

import com.phdata.di.helper.{ConfigProperties, GenericHelper}
import com.phdata.di.service.AirLineDataService
import com.phdata.di.utility.GenericUtility
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.Test
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito.spy

class AirlineDataPipelineTest extends Serializable {

  val spark: SparkSession = SparkSession.builder.appName("AirlineDataServiceTest").master("local[*]").getOrCreate()
  val sqlContext = spark.sqlContext

  val spyGenericUtility: GenericUtility = spy( new GenericUtility() ) // Utility spy
  val genericHelper: GenericHelper = spy(new GenericHelper(spyGenericUtility))

  val flightDataDF: DataFrame = sqlContext.createDataFrame( Seq( FlightData( 2015,2,1,7,"UA",1142,"N67815","DEN","LAS",1115,1137,22,59,1236,118,147,81,628,1257,7,1213,1304,51,0,0,"",29,0,22,0,0) ,
    FlightData(2015,2,2,7,"DL",1142,"N67816","DEN","LAS",1115,1137,22,59,1236,118,147,81,628,1257,7,1213,1304,51,0,0,"",29,0,22,0,0),
    FlightData(2015,2,3,7,"AA",1142,"N67817","DEN","LAS",1115,1137,22,59,1236,118,147,81,628,1257,7,1213,1304,51,0,0,"",29,0,22,0,0)
  ))

  val airportDataDF: DataFrame = sqlContext.createDataFrame( Seq( AirportData( "DEN","Denver International Airport","Denver","CO","USA","39.85841","-104.66700") ,
    AirportData("LAS","McCarran International Airport","Las Vegas","NV","USA","36.08036","-115.15233")
  ))

  val airlineDataDF: DataFrame = sqlContext.createDataFrame( Seq( AirlineData( "UA","United Air Lines Inc.") ,
    AirlineData("AA","American Airlines Inc."),
    AirlineData("DL","Delta Air Lines Inc.")
  ))


  @Test
  def prepareAirportDataTest(): Unit = {
    Mockito.doReturn(airportDataDF).when( spyGenericUtility ).prepareDataFrameFromFile( any[String], any[String], any[String], any[SQLContext], any[StructType] )
    val finalDF = genericHelper.prepareAirportData( "/testLocation", spark)
    assertNotNull(finalDF)
  }

  @Test
  def prepareAirlineDataTest(): Unit = {
    Mockito.doReturn(airlineDataDF).when( spyGenericUtility ).prepareDataFrameFromFile( any[String], any[String], any[String], any[SQLContext], any[StructType] )
    val finalDF = genericHelper.prepareAirlineData( "/testLocation", spark)
    assertNotNull(finalDF)
  }

  @Test
  def prepareFlightDataTest(): Unit = {
    Mockito.doReturn(flightDataDF).when( spyGenericUtility ).prepareDataFrameFromFile( any[String], any[String], any[String], any[SQLContext], any[StructType] )
    val finalDF = genericHelper.prepareFlightData( "/testLocation", spark)
    assertNotNull(finalDF)
  }

  @Test
  def prepareAirlineIncrementalDataTest(): Unit = {
    val finalDF = genericHelper.prepareAirlineIncrementalData( airlineDataDF, airportDataDF, flightDataDF, null, spark)
    assertNotNull(finalDF)
  }

  case class AirportData ( IATA_CODE: String, AIRPORT: String,
                           CITY: String, STATE: String,  COUNTRY: String,
                           LATITUDE: String, LONGITUDE: String)


  case class AirlineData (IATA_CODE: String,AIRLINE: String)
  case class FlightData  (YEAR: Integer, MONTH: Integer,
                          DAY: Integer, DAY_OF_WEEK: Integer,
                          AIRLINE: String, FLIGHT_NUMBER: Integer,
                          TAIL_NUMBER: String, ORIGIN_AIRPORT: String,
                          DESTINATION_AIRPORT: String, SCHEDULED_DEPARTURE: Integer,
                          DEPARTURE_TIME: Integer, DEPARTURE_DELAY: Integer,
                          TAXI_OUT: Integer, WHEELS_OFF: Integer,
                          SCHEDULED_TIME: Integer, ELAPSED_TIME: Integer,
                          AIR_TIME: Integer,  DISTANCE: Integer,
                          WHEELS_ON: Integer, TAXI_IN: Integer,
                          SCHEDULED_ARRIVAL: Integer, ARRIVAL_TIME: Integer,
                          ARRIVAL_DELAY: Integer,  DIVERTED: Integer,
                          CANCELLED: Integer, CANCELLATION_REASON: String,
                          AIR_SYSTEM_DELAY: Integer, SECURITY_DELAY: Integer,
                          AIRLINE_DELAY: Integer, LATE_AIRCRAFT_DELAY: Integer,
                          WEATHER_DELAY: Integer)

}
