package it.eng.au.ERP.trasformations.IP

import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.schema.au.vAggreagazioneMisureIPSchema
import it.eng.au.ERP.schema.erp.erpAggregatoIPOSchema
import it.eng.au.ERP.schema.rcu.RcuAziendaPSchema
import it.eng.au.ERP.utility.args.ERPArgsConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CalcoloPrelevatoPuntiPrelievoOrariIPTrasformation {

  val d_data_timestamp = "d_data_timestamp"

  def calcoloPrelevatoPuntiPrelievoOrariIP(dsvAggregazioneMisureIP: DataFrame,
                                           dsRcuAziendaP: Dataset[RcuAziendaPModel],
                                           args: ERPArgsConfig,
                                           podExcluded: List[String],
                                           timestamp: Long,
                                           year: Option[Int],
                                           month: Option[Int],
                                           annomese: Option[String],
                                           area: Option[String],
                                           singola_piva_distributore: Option[String]
                                          )
                                          (implicit spark: SparkSession): DataFrame = {

    val dsvAggregazioneMisureIPFiltered = CalcoloPrelevatoPuntiPrelievoOrariIP
      .aggregazioneMisureIPFiltered(
        dsvAggregazioneMisureIP,
        annomese,
        year,
        month,
        area,
        podExcluded
      )

    val dsvAggregazioneMisureIPPrepared = dsvAggregazioneMisureIPFiltered
      // Build giorno/anno/mese directly from d_data without reusing intermediate cols to avoid name shadowing
      .withColumn("__giorno_iso__", format_string(
        "%s-%s-%s",
        date_format(col(vAggreagazioneMisureIPSchema.d_data), "yyyy"),
        date_format(col(vAggreagazioneMisureIPSchema.d_data), "MM"),
        date_format(col(vAggreagazioneMisureIPSchema.d_data), "dd")
      ))
      .withColumn(erpAggregatoIPOSchema.giorno, col("__giorno_iso__"))
      .withColumn(erpAggregatoIPOSchema.anno, date_format(col(vAggreagazioneMisureIPSchema.d_data), "yyyy"))
      .withColumn(erpAggregatoIPOSchema.mese, date_format(col(vAggreagazioneMisureIPSchema.d_data), "MM"))
      .drop("__giorno_iso__")

    val dsRcuAziendaPFiltered = dsRcuAziendaP
      .withColumnRenamed(RcuAziendaPSchema.t_piva, erpAggregatoIPOSchema.piva_distr)
      .withColumnRenamed(RcuAziendaPSchema.t_rag_soc, erpAggregatoIPOSchema.rag_soc_distr)
      .select(
        col(RcuAziendaPSchema.n_id_azienda),
        col(erpAggregatoIPOSchema.piva_distr),
        col(erpAggregatoIPOSchema.rag_soc_distr)
      )

    val dfJoin = dsvAggregazioneMisureIPPrepared.join(
      broadcast(dsRcuAziendaPFiltered),
      col(vAggreagazioneMisureIPSchema.n_id_dis) === col(RcuAziendaPSchema.n_id_azienda),
      "left"
    )

    val normalizedPiva = singola_piva_distributore.flatMap(piva => Option(piva.trim)).filter(_.nonEmpty)
    val dfJoinFiltered = normalizedPiva match {
      case Some(pivaValue) => dfJoin.filter(col(erpAggregatoIPOSchema.piva_distr) === lit(pivaValue))
      case None => dfJoin
    }

    val quartHourlySourceColumns = vAggreagazioneMisureIPSchema.values.toList
      .map(_.toString)
      .filter(name => name.startsWith("h") && name.contains("_q"))

    val quartHourlyTargetColumns = erpAggregatoIPOSchema.values.toList
      .map(_.toString)
      .filter(_.matches("q\\d+"))

    require(
      quartHourlySourceColumns.length == quartHourlyTargetColumns.length,
      s"Mismatch between quarter-hour source (${quartHourlySourceColumns.length}) and target (${quartHourlyTargetColumns.length}) columns for IP aggregation."
    )

    val aggregationColumns = quartHourlySourceColumns.zip(quartHourlyTargetColumns).map {
      case (source, target) => sum(col(source).cast("double")).alias(target)
    }

    val dfAggregation = dfJoinFiltered.groupBy(
      col(erpAggregatoIPOSchema.anno),
      col(erpAggregatoIPOSchema.mese),
      col(erpAggregatoIPOSchema.giorno),
      col(vAggreagazioneMisureIPSchema.t_area),
      col(erpAggregatoIPOSchema.piva_distr),
      col(erpAggregatoIPOSchema.rag_soc_distr)
    ).agg(aggregationColumns.head, aggregationColumns.tail: _*)
      .withColumn(erpAggregatoIPOSchema.executionid, lit(timestamp))
      .withColumnRenamed(vAggreagazioneMisureIPSchema.t_area, erpAggregatoIPOSchema.area)
      .drop(vAggreagazioneMisureIPSchema.annomese)

    dfAggregation.selectExpr(erpAggregatoIPOSchema.getValues: _*)
  }

}
