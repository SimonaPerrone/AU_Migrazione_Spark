package it.au.misure.calcolo_capacita.utility

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.security.Permission

trait ForUnitTest extends AnyFunSuite with MockFactory with BeforeAndAfter {

  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _hiveContext: HiveContext = _

  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: HiveContext = _hiveContext


  private def initializeSpark(appName: String): Unit = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")

    _sc = SparkContext.getOrCreate(conf)
    _sqlContext = SQLContext.getOrCreate(_sc)
    _hiveContext = new HiveContext(sc)

//    _hiveContext.setConf("spark.driver.memoryOverhead","128mb")
//    _hiveContext.setConf("spark.default.parallelism", "8")
//    _hiveContext.setConf("spark.sql.shuffle.partitions", "8")

  }



  before {
    println("before")
    initializeSpark("test")
    System.setSecurityManager(new NoExitSecurityManager())
  }

  after {
    println("after")
    System.setSecurityManager(null)
  }

  protected class ExitException(val status: Int) extends SecurityException("There is no escape!") {
  }
  private class NoExitSecurityManager extends SecurityManager {
    override def checkPermission(perm: Permission): Unit = {
      // allow anything.
    }
    override def checkPermission(perm: Permission, context: Any): Unit = {
    }
    override def checkExit(status: Int): Unit = {
      super.checkExit(status)
      throw new ExitException(status)
    }
  }

}
