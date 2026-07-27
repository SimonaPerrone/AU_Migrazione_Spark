package it.eng.au.aggregatoreConsumiCommon.utility

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}

import scala.util.Try

class CustomSparkListener extends SparkListener {
  val path: String = Environment.getIsilonBasepathTmp + "/tmp/"

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    Environment.sparkContext.parallelize(List(1), 1).foreach(_ => {
      import sys.process._
      Try(s"chmod 777 -R $path" !)
    })
    super.onApplicationEnd(applicationEnd)
  }
}
