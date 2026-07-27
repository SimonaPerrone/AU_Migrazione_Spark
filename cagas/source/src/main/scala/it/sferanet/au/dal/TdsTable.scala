package it.sferanet.au.dal

import it.sferanet.au.model.{Tds, TdsFields}
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Date

class TdsTable(inputPath: String, fields: TdsFields) extends Serializable {

  def this(inputPath: String) = {
    this(inputPath, TdsTable.flowFields)
  }

  def get(): RDD[Tds] = {
    Environment.getSqlContext.read
      .parquet(inputPath)
      .rdd
      .map(r => {
        val dataString = r.getAs("data_creazione").toString
        val date = if (dataString.length >= 17)
          Constants.getDate(Constants.getFormatter("dd/MM/yy HH:mm:ss"), dataString.substring(0, 17))
        else None
        Tds(
          r.getAs[Boolean]("valid"),
          r.getAs("cod_pdr").toString,
          r.getAs("cat_uso").toString,
          r.getAs("classe_prelievo").toString,
          if (date.isDefined) date.get else new Date(0)
        )
      })
      .filter(_.isValid)
  }


}

object TdsTable {
  val flowFields: TdsFields = TdsFields(
    "cod_pdr",
    "cat_uso",
    "classe_prelievo",
    "data_creazione"
  )

  def format = new SimpleDateFormat("yyyy-MM-dd")
}
