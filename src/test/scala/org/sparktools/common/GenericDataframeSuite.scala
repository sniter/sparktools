package org.sparktools.common

import org.apache.spark.sql.DataFrame

trait GenericDataframeSuite {

  def checkAnswer(df:DataFrame, expected: List[_]): Unit = {
    assert(df.collect().toList == expected)
  }

}
