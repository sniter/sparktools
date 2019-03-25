package org.sparktools.common

import org.apache.spark.sql.SparkSession

trait SparkExtension {
  def spark: SparkSession
}
