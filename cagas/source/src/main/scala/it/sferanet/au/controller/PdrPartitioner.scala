package it.sferanet.au.controller

import org.apache.spark.Partitioner

class PdrPartitioner(numberPartitions: Int) extends Partitioner {

  override def numPartitions: Int = this.numberPartitions

  override def getPartition(key: Any): Int = {
    val k = try {
      key.toString.toLong
    } catch {
      case _: Throwable =>
        val number = key.toString.split("\\D+").filter(_.nonEmpty).mkString("") // prendo solo la parte numerica della stringa
        if (number.isEmpty) 0 else number.toLong // se non c'è una parte numerica mettiamo la partizione 0 di default
    }
    (k % numPartitions).toInt
  }
}
