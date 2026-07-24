package it.au.misure.calcolo_capacita.utility

import it.au.misure.calcolo_capacita.component.schema.CalcoloConsumiSbgSchema
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert
trait Checker {

  def checksValues(checks: Tuple2[String, String]*): Unit = {
    val checks_2=checks
    checks_2.foreach((pair) => Assert.assertTrue(pair._1 == pair._2))
  }

  def getValueCalculated(df: DataFrame, pdrCol: String, pdr: String, colName: String): String = {

    val r = df.filter(col(pdrCol) === lit(pdr)).select(colName)
    val v = r.take(1)(0).get(0)
    if (v == null)
      "null"
    else v.toString
  }
  def getValueCalculatedOpti(rows:Array[Row], nCol: Int): String = {

    val v = rows(0).get(nCol)
    if (v == null)
      "null"
    else v.toString
  }

  def checkIfExistsPdr(df: DataFrame, pdrCol: String, pdr: String, expected: Boolean): Unit = {
    val r = df.filter(col(pdrCol) === lit(pdr))
    Assert.assertTrue((r.count() != 1) == !expected)

  }

  def checkIfExistsDateForPdr(df: DataFrame, pdrCol: String, pdr: String, annoMese: String, giorno: String, isThere: Boolean): Unit = {
    val r = df.filter((col(pdrCol) === lit(pdr)) and
      (col( CalcoloConsumiSbgSchema.annomese_rif) === lit(annoMese)) and
      (col(CalcoloConsumiSbgSchema.giorno) === lit(giorno)))
    Assert.assertTrue((r.count() == 1) == isThere)

  }


}
