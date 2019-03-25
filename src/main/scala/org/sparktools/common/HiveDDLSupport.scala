package org.sparktools.common

import org.apache.spark.sql._

abstract class HiveDDLSupport extends SparkExtension with Types {

  protected def partitionToSeq(partition: String): Partition =
    partition
      .split("/")
      .map(_.split("="))
      .map(v => v(0) -> v(1)).toSeq

  protected def partitionToSqlExpr(partition: Partition): String =
    partition
      .map { case (column, value) => s"$column='$value'"}
      .mkString(", ")

  def tablePartitions(table: String): Seq[Partition] =
    spark.sql(s"SHOW PARTITIONS $table")
      .collect()
      .map{ case Row(part: String) => partitionToSeq(part) }
      .toSeq

  def tableColumns(table: String): Seq[ColumnDescr] =
    spark.sql(s"DESCRIBE $table")
      .collect()
      .map{ case Row(column: String, colType: String) => column -> colType}

  def dropPartition(table: String, partition: Partition): Unit =
    spark.sql(s"ALTER TABLE DROP PARTITION (${ partitionToSqlExpr(partition) })")


}
