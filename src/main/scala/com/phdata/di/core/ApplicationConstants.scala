package com.phdata.di.core

object ApplicationConstants {

  var AIRLINE_DATA_TABLE = "AIRLINE_AGGREGATED"
  var AIRLINE_ETL_HISTORY_QUERY = "(select * from pipeline_run_history where IS_ACTIVE = 'Y' and SKIP_EXECUTION = 'N') a"

}