package com.phdata.di.utility

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

class SparkInitializer {
  /**
   * Initialize Spark Session
   * @param envType
   * @param appName
   * @param awsAccessKeyId
   * @param awsSecretAccessKey
   * @return SparkSession
   */
  def sparkSession(envType: String, appName: String): SparkSession = {
    if ((envType == "DEV") || (envType == "QA")) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"))
      val sparkInitializer = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
      sparkInitializer.conf.set("spark.sql.session.timeZone", "America/New_York")
      sparkInitializer
    } else {
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"))
      val sparkInitializer = SparkSession.builder.appName(appName).getOrCreate()
      sparkInitializer.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
      sparkInitializer
      }
  }

  /**
   * Initialize Spark Session with AWS Access Cred
   * @param envType
   * @param appName
   * @param awsAccessKeyId
   * @param awsSecretAccessKey
   * @return SparkSession
   */
  def sparkSession(envType: String, appName: String, awsAccessKeyId: String, awsSecretAccessKey: String): SparkSession = {

    if ((envType == "DEV") || (envType == "QA")) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"))
      val sparkInitializer = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
      sparkInitializer.conf.set("spark.sql.session.timeZone", "America/New_York")
      sparkInitializer
    } else {
      val sparkInitializer = SparkSession.builder.appName(appName).getOrCreate()
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Los_Angeles"))
      sparkInitializer.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sparkInitializer.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKeyId)
      sparkInitializer.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretAccessKey)
      sparkInitializer.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
      sparkInitializer
    }
  }
}
