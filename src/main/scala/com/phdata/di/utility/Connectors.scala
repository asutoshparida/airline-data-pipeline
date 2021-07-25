package com.phdata.di.utility

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.phdata.di.helper.ConfigProperties

class Connectors() {

  /**
   * Dump data to snowflake either in "append" or "overwrite" mode.
   * @param config
   * @param df
   * @param table
   * @param saveMode
   */
  def writeToSnowFlakes(config: ConfigProperties, df: DataFrame, table: String, saveMode: SaveMode): Unit = {
    val sfOptions: Map[String, String] = Map(
      "sfURL" -> config.sfURL,
      "sfAccount" -> config.sfAccount,
      "sfUser" -> config.sfUser,
      "sfPassword" -> config.sfPassword,
      "sfDatabase" -> config.sfDatabase,
      "sfSchema" -> config.sfSchema,
      "sfRole" -> config.sfRole)

    df.write
      .format("snowflake")
      .options(sfOptions)
      .option("dbtable", table)
      .mode(saveMode)
      .save()
  }

  /**
   * Read data from Snowflake with input query
   * @param config
   * @param table
   * @param spark
   * @param query
   * @return
   */
  def readFromSnowFlakes(config: ConfigProperties, table: String, spark: SparkSession, query : String): DataFrame = {
    val sfOptions: Map[String, String] = Map(
      "sfURL" -> config.sfURL,
      "sfAccount" -> config.sfAccount,
      "sfUser" -> config.sfUser,
      "sfPassword" -> config.sfPassword,
      "sfDatabase" -> config.sfDatabase,
      "sfSchema" -> config.sfSchema,
      "sfRole" -> config.sfRole)

    spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", query)
      .load()
  }

  /**
   * Read data from postgresql using pushdown query
   * @param configMap
   * @param pushdownQuery
   * @param sqlContext
   * @return
   */
  @throws(classOf[Exception])
  def readFromDB(config: ConfigProperties, pushdownQuery: String, sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    val sqlHost: String = config.readDBURL
    val database: String = config.database
    val userName: String = config.dbUser
    val password: String = config.dbPass
    val prop = new java.util.Properties()
    prop.put("driver", "org.postgresql.Driver")
    prop.put("user", userName)
    prop.put("password", password)
    val df = sqlContext.read.jdbc(url = sqlHost + database, pushdownQuery, properties = prop)
    return df
  }

}