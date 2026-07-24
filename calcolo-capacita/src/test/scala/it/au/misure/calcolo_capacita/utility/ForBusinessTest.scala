package it.au.misure.calcolo_capacita.utility

import it.au.misure.calcolo_capacita.component.schema.ClgPdrCapacitaSchema
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

trait ForBusinessTest extends AnyFunSuite with MockFactory with BeforeAndAfter {

  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _hiveContext: HiveContext = _

  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: HiveContext = _hiveContext

  val cols:List[String] = ClgPdrCapacitaSchema.getValues
  var mapping: mutable.Map[String, Int] = mutable.HashMap.empty
  var i = 0
  ClgPdrCapacitaSchema.getValues.foreach((col) => {
    mapping = mapping + (col -> i)
    i = i + 1
  })
  val executionId: String = LocalDateTime.now().toString("yyyyMMddHHmmss")

  private def initializeSpark(appName: String): Unit = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    _sc = SparkContext.getOrCreate(conf)
    _sqlContext = SQLContext.getOrCreate(_sc)
    _hiveContext = new HiveContext(sc)

  }


  before {
    println("before")

    initializeSpark("test")
  }

  after {
    println("after")
  }

}
