package it.eng.au.aggregatoreConsumiCommon.controller.impl.aggregatorTripla.elencoRemi

import it.eng.au.aggregatoreConsumiCommon.controller.traits.RunnableAggregatorPerfomance
import it.eng.au.aggregatoreConsumiCommon.schema.AggregatoTriplaElencoRemiSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, current_date, lit}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

trait ElencoRemiDettaglioAggregatoTripla extends RunnableAggregatorPerfomance {
  override val operationName = "DISTR_IT_REMI"

  def getElencoRemi(df: DataFrame, remiAnagrafica: DataFrame, gestTrasporto: DataFrame, vRcugasIt: DataFrame, connessioniDistr2: DataFrame): DataFrame = {

    // Sottoquery conn con filtri sulle date
    val conn = connessioniDistr2
      .filter(
        current_date() >= coalesce(col("d_data_inizio_conn"), lit("1900-01-01")) &&
          current_date() <= coalesce(col("d_data_fine_conn"), lit("2300-01-01"))
      )
      .select("t_remi", "t_piva_distr")
      .distinct()

    // Join principali
    val remiOnline = remiAnagrafica
      .join(gestTrasporto, remiAnagrafica("n_id_remi_anagrafica") === gestTrasporto("n_id_remi_anagrafica"))
      .join(vRcugasIt, gestTrasporto("n_id_it") === vRcugasIt("n_id_it"))
      .join(conn, remiAnagrafica("t_remi") === conn("t_remi"), "left")
      .filter(
        current_date() >= coalesce(gestTrasporto("d_data_inizio"), lit("1900-01-01")) &&
          current_date() <= coalesce(gestTrasporto("d_data_fine"), lit("2300-01-01"))
      )
      .select(
        remiAnagrafica("t_remi"),
        vRcugasIt("t_piva").alias("piva_it"),
        conn("t_piva_distr").alias("piva_distr")
      )
      .withColumnRenamed("t_remi", AggregatoTriplaElencoRemiSchema.codice_remi)
      .withColumnRenamed("piva_distr", AggregatoTriplaElencoRemiSchema.piva_distr)
      .withColumnRenamed("piva_it", AggregatoTriplaElencoRemiSchema.piva_it)
      .distinct()

    df.select(AggregatoTriplaElencoRemiSchema.codice_remi.toString, AggregatoTriplaElencoRemiSchema.piva_rdb.toString, AggregatoTriplaElencoRemiSchema.annomese.toString).distinct
      .join(remiOnline, Seq(AggregatoTriplaElencoRemiSchema.codice_remi.toString), "inner")
  }

  override def getCsvOutputPath(baseName: String, mapKey: Map[String, String], date: LocalDateTime, publicationType: String, sessionName: String, counterCsv: String, annoMese: String, optionalOperationName: Option[String] = None): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaPathFolderHead = mapKey(keyFields.head)
    val pivaNameFile = keyFields.map(key => mapKey(key)).mkString("_")

    s"/${baseName}_$pivaPathFolderHead/$year/$month/${pivaNameFile}_${operationName}_ONLINE_${timestamp}_${counterCsv}.csv"
  }

  //Not used
  override def getAggregato(df: DataFrame): DataFrame = Environment.sqlContext.emptyDataFrame
}
