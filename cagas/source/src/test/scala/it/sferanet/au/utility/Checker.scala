package it.sferanet.au.utility

import org.apache.spark.sql.Row
import org.junit.Assert

trait Checker {

  def checksValues(checks: Tuple2[String, String]*): Unit = {
    checks.foreach((pair) => Assert.assertTrue(pair._1 == pair._2))
  }

  def getValueCalculatedOpti(rows: Array[Row], nCol: Int): String = {

    val v = rows(0).get(nCol)
    if (v == null)
      "null"
    else v.toString
  }

}
