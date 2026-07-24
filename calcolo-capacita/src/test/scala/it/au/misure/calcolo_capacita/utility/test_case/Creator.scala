package it.au.misure.calcolo_capacita.utility.test_case

import org.apache.spark.sql.{DataFrame, SQLContext}

trait Creator {


  def getMeasures(implicit sqlContext: SQLContext): DataFrame

  def getAnagrafica(implicit sqlContext: SQLContext): DataFrame

  def getMisureInPerimetro(implicit sqlContext: SQLContext): DataFrame

  def getRCUGasMassivo(implicit sqlContext: SQLContext): Option[DataFrame]

}
