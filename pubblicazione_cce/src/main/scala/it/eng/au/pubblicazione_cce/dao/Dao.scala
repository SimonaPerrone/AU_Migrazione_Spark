package it.eng.au.pubblicazione_cce.dao

import org.apache.spark.sql.DataFrame

trait Dao {
  
  def read(): DataFrame
  def write(df: DataFrame): Unit

}
