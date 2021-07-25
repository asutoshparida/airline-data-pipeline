package com.phdata.di.contrloller

import com.phdata.di.utility.SparkInitializer
import com.phdata.di.helper.ConfigProperties
import com.phdata.di.service.AirLineDataService
import com.typesafe.scalalogging.LazyLogging

object AirLineDataController extends LazyLogging  {

  def main(args: Array[String]): Unit = {
    val environment = "DEV"//args(0)
    logger.info(s"Start AirLineDataController with environment :" + environment)
    logger.info(s"Load all pipeline configurations :")
    val config: ConfigProperties = new ConfigProperties().setConfigPropertey()
    logger.info(s"Initializing Spark Session :")
    val spark = new SparkInitializer().sparkSession(environment, "SparkInitializer")
    val airLineDataService = new AirLineDataService(config, spark)
    airLineDataService.processAirLineDataIncrementally()
  }
}