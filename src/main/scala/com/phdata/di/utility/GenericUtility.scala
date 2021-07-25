package com.phdata.di.utility

import com.phdata.di.helper.ConfigProperties
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.sql.DriverManager
import java.time.{DayOfWeek, LocalDate, LocalDateTime}
import scala.sys.process._

class GenericUtility extends LazyLogging {

  /**
   * Prepare datFrame from filepath.
   * @param filePath
   * @param separator
   * @param quote
   * @param context
   * @param data_schema
   * @return
   */
  def prepareDataFrameFromFile(filePath: String, separator: String, quote: String, context: org.apache.spark.sql.SQLContext, data_schema: org.apache.spark.sql.types.StructType): DataFrame = {

    val df: DataFrame = context.read
      .format("com.databricks.spark.csv")
      .option("delimiter", separator)
      .option("quote", quote)
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(data_schema)
      .load(filePath)

    return df
  }

  def executeInsertUpdateSQL(db_url: String, username: String, password: String, queryStmt: String, statementType: String) = {
    val connection = DriverManager.getConnection(db_url, username, password)
    val statement = connection.createStatement
    if(statementType.equalsIgnoreCase("update")) {
      statement.executeUpdate(queryStmt)
    } else if(statementType.equalsIgnoreCase("insert")) {
      statement.execute(queryStmt)
    }
  }

  /**
   * Update pipeline_run_history for that etl run.
   * @param config
   * @param statement
   * @return
   */
  def updateHistoryTable(config: ConfigProperties, etlName: String, runId: Int) = {
    try {
      val airlineETLMonitorUpdateQuery = s"update pipeline_run_history set IS_ACTIVE = 'N' Where ID = ${runId} AND ETL_NAME = '${etlName}'"
      logger.info(s"GenericUtility # updateHistoryTable : $airlineETLMonitorUpdateQuery")
      executeInsertUpdateSQL(config.readDBURL + config.database, config.dbUser, config.dbPass, airlineETLMonitorUpdateQuery, "update");
    } catch {
      case exp: Exception => {
        logger.error(s"GenericUtility # updateHistoryTable # Error :Updating pipeline_run_history Table:" + exp)
        throw exp
      }
    }
  }

  def getFieldValue(rowData: Row, field: String): String = {
    try {
      rowData.get(rowData.fieldIndex(field)).toString
    } catch {
      case _: Exception =>
        null
    }
  }

  /**
   * Logs exception for that run to exception_log table
   * @param config
   * @param etlName
   * @param client
   * @param exp
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def logException(config: ConfigProperties, etlName: String, runId: Int, exceptionMessage: String ): Unit = {
    val sqlHost: String = config.readDBURL
    val database: String = config.database
    val userName: String = config.dbUser
    val password: String = config.dbPass

    try {
      val logQuery =
        s"""INSERT INTO exception_log (date_time, etl_name, run_id, exception_message)
        VALUES ( current_timestamp, '$etlName', '$runId', '${exceptionMessage.replaceAll("'", "''")}')"""
      executeInsertUpdateSQL(sqlHost + database, userName, password, logQuery, "insert")
    } catch {
      case exp: Exception =>
        logger.error(s"GenericUtility # logException # Error :Updating exception_log Table: Failed to update" + exp)
        exp.printStackTrace()
    }
  }

  /**
   * Insert new entry for the etl with updated EVENT_DATE in pipeline_run_history Table
   * @param config
   * @param etlName
   * @param runId
   * @param eventDate
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def insertPipelineHistory(config: ConfigProperties, etlName: String, runId: Int, eventDate: String ): Unit = {
    val sqlHost: String = config.readDBURL
    val database: String = config.database
    val userName: String = config.dbUser
    val password: String = config.dbPass
    logger.info(s"GenericUtility # insertPipelineHistory with eventDate : $eventDate")
    try {
      val logQuery =
        s"""INSERT INTO pipeline_run_history (ETL_NAME , SKIP_EXECUTION, FULL_LOAD, IS_ACTIVE, run_date, filter_col1_name, filter_col1_value)
        VALUES ( '$etlName', 'N', 'N', 'Y', current_timestamp, 'EVENT_DATE','${eventDate}')"""
      executeInsertUpdateSQL(sqlHost + database, userName, password, logQuery, "insert")
    } catch {
      case exp: Exception =>
        logger.error(s"GenericUtility # insertPipelineHistory # Error :inserting pipeline_run_history Table: Failed to Insert" + exp)
        exp.printStackTrace()
        throw exp
    }
  }

  def getDateTime(): String = {
    try {
      return s"${LocalDateTime.now.getDayOfMonth()}/${LocalDateTime.now.getMonthValue()}/${LocalDateTime.now.getYear()} ${LocalDateTime.now.getHour()}:${LocalDateTime.now.getMinute()}:${LocalDateTime.now.getSecond()}"
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        return null
      }
    }
  }

  def execCommands(cmd: String): String = {
    return Seq("bash", "-c", cmd) !!
  }

}