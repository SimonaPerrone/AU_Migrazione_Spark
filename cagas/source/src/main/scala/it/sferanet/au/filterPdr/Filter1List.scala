package it.sferanet.au.filterPdr

import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD

class Filter1List extends FilterPdr {
  override def getPdrs: RDD[String] = {
    val filePath = Environment.getFilterPdrCsvPath

    val pdrSet = Environment.getSparkContext.textFile(filePath)
    pdrSet
  }
}
