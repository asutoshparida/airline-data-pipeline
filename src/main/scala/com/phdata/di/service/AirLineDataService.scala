package com.phdata.di.service

import com.phdata.di.core.ApplicationConstants
import com.phdata.di.helper.{ConfigProperties, GenericHelper}
import com.phdata.di.utility.{Connectors, GenericUtility}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{max}

class AirLineDataService(config: ConfigProperties, sparkSession: SparkSession) extends LazyLogging {

  /**
   * This Service function does the data loading, cleanup & final curation before pushing it to snowplow
   *
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def processAirLineDataIncrementally() = {
    val genericUtility: GenericUtility = new GenericUtility()
    val genericHelper = new GenericHelper(genericUtility)
    var runId: Int = 0
    var etlName: String = null
    logger.info(s"AirLineDataService# processAirLineDataIncrementally #Load CSV data from FileStore")
    try {
      val airlineDataFrame = genericHelper.prepareAirlineData(config.airlineDataPath, sparkSession)
      val airportDataFrame = genericHelper.prepareAirportData(config.airportDataPath, sparkSession)
      val stageFlightsDF = genericHelper.prepareFlightData(config.flightsDataPath, sparkSession)

      val connector: Connectors = new Connectors()
      var finalAirlinesDataFrame: DataFrame = null
      val airlineETLMonitorQuery = ApplicationConstants.AIRLINE_ETL_HISTORY_QUERY
      val monitorDf = connector.readFromDB(config, airlineETLMonitorQuery, sparkSession.sqlContext)
      val monitorDataList = monitorDf.collectAsList
      val listLength = monitorDataList.size()
      logger.info(s"AirLineDataService# processAirLineDataIncrementally # Application configuration is Over")
      if (listLength > 0) {
        val rowData = monitorDataList.get(0)
        val isFullLoad = genericUtility.getFieldValue(rowData, "full_load")
        runId = genericUtility.getFieldValue(rowData, "id").toInt
        etlName = genericUtility.getFieldValue(rowData, "etl_name")
        val incrementalColumnName = genericUtility.getFieldValue(rowData, "filter_col1_name")
        val incrementalColumnValue = genericUtility.getFieldValue(rowData, "filter_col1_value")
        logger.info(s"AirLineDataService# processWeatherDataAndDumpAsParquet # isFullLoad :" + isFullLoad + "# runId :" + runId + "# etlName :" + etlName + "# incrementalColumnName :" + incrementalColumnName + "# incrementalColumnValue :" + incrementalColumnValue)

        if (isFullLoad.equalsIgnoreCase("Y")) {
          /**
           * AirLineDataService# processWeatherDataAndDumpAsParquet # Logic for Full load(Overwrite)
           */
          finalAirlinesDataFrame = genericHelper.prepareAirlineIncrementalData(airlineDataFrame, airportDataFrame, stageFlightsDF, null, sparkSession)
          connector.writeToSnowFlakes(config, finalAirlinesDataFrame, ApplicationConstants.AIRLINE_DATA_TABLE, SaveMode.Overwrite)
        } else if (incrementalColumnName.equalsIgnoreCase("EVENT_DATE")) {
          /**
           * AirLineDataService# processWeatherDataAndDumpAsParquet # Logic for Delta load(Append)
           */
          finalAirlinesDataFrame = genericHelper.prepareAirlineIncrementalData(airlineDataFrame, airportDataFrame, stageFlightsDF, incrementalColumnValue, sparkSession)
          connector.writeToSnowFlakes(config, finalAirlinesDataFrame, ApplicationConstants.AIRLINE_DATA_TABLE, SaveMode.Append)
        }

        logger.info(s"AirLineDataService# processWeatherDataAndDumpAsParquet # updating pipeline_run_history table:" )
        genericUtility.updateHistoryTable(config, etlName, runId)

        logger.info(s"AirLineDataService# processWeatherDataAndDumpAsParquet # insert pipeline_run_history table:" )
        val maxEventDate = finalAirlinesDataFrame.agg(max("EVENT_DATE")).first().get(0).toString
        genericUtility.insertPipelineHistory(config, etlName, runId, maxEventDate)

        logger.info(s"AirLineDataService# processWeatherDataAndDumpAsParquet # data load to AIRLINE_AGGREGATED completed :" )
      }
    } catch {
      case exp: Exception => {
        val errorLog = s"AirLineDataService# processAirLineDataIncrementally #Failed while processing Airline data. Exp.: ${exp.getMessage}"
        logger.error(errorLog)
        genericUtility.logException(config, etlName, runId, errorLog)
      }
    }
  }

}