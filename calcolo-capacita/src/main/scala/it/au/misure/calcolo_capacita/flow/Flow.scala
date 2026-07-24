package it.au.misure.calcolo_capacita.flow


import org.apache.spark.sql.{DataFrame, SQLContext}


trait Flow {


  def run()(implicit SQLContext: SQLContext):DataFrame

  def write(dataFrame: DataFrame):Unit
}
