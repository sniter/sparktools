package org.sparktools.common

import org.apache.spark.sql.SparkSession

object implicits {

  implicit class SparkHelper(val spark: SparkSession)
    extends HiveDDLSupport

}
