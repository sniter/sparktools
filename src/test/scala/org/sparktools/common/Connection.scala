package org.sparktools.common

import org.apache.spark.sql.SparkSession

object Connection {

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkTools")
    .master("local")
    .config("spark.driver.host","localhost")
    .config("spark.sql.catalogImplementation","hive")
    .getOrCreate()

}
