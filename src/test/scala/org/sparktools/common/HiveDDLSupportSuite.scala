package org.sparktools.common

import org.apache.spark.sql.Row
import org.scalatest._

class HiveDDLSupportSuite extends FunSuite with GenericDataframeSuite {
  import Connection.spark
  import Connection.spark.sql
  import implicits._

  test("Drop Partition Test"){

    sql("DROP TABLE IF EXISTS sales")

    sql("CREATE TABLE sales(id INT, country STRING, quarter STRING) USING HIVE PARTITIONED BY (country, quarter)")

    for (country <- Seq("US", "CA", "KR")) {
      for (quarter <- 1 to 2) {
        sql(s"ALTER TABLE sales ADD PARTITION (country = '$country', quarter = '$quarter')")
      }
    }

    spark.dropPartition("sales", Seq("country" -> "KR", "quarter" -> "1"))

    checkAnswer(sql("SHOW PARTITIONS sales"),
      Row("country=CA/quarter=1") ::
      Row("country=CA/quarter=2") ::
      Row("country=KR/quarter=2") ::
      Row("country=US/quarter=1") ::
      Row("country=US/quarter=2") :: Nil)

    spark.dropPartition("sales", Seq("country" -> "CA"))

    checkAnswer(sql("SHOW PARTITIONS sales"),
      Row("country=KR/quarter=2") ::
      Row("country=US/quarter=1") ::
      Row("country=US/quarter=2") :: Nil)

    spark.dropPartition("sales", Seq("quarter" -> "2"))

    checkAnswer(sql("SHOW PARTITIONS sales"),
      Row("country=US/quarter=1") :: Nil)
  }

}
