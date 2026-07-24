package it.eng.au.gse.calcoloMensile.controller

import it.eng.au.gse.common.dao.GsePerimetroDao.t_cliente
import it.eng.au.gse.calcoloMensile.schema.gse.GseRichiestaSchema
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.Properties
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

import java.time.YearMonth
import java.time.format.DateTimeFormatter
import scala.util.Try
import it.eng.au.gse.common.utility.Constants._

object Prepare {
  def preparePodPerimeter(perimetroDf: DataFrame): DataFrame = {
    perimetroDf
      .where(col(GsePerimetroSchema.t_valido) === STATO_VALIDO)
      .selectExpr("*", s"stack(2, '${GsePerimetroSchema.t_cf_cliente}', ${GsePerimetroSchema.t_cf_cliente}, '${GsePerimetroSchema.t_piva_cliente}', ${GsePerimetroSchema.t_piva_cliente}) as (source, $t_cliente)")
      .select(
        GsePerimetroSchema.t_cod_pod,
        GsePerimetroSchema.t_mese_anno,
        GsePerimetroDao.t_cliente
      )
      .distinct()
  }

  def prepareRequests(richiesteDf: DataFrame): (DataFrame, List[(Int, Int)]) = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val yearMonth = Properties.getYearMonth

    val newRequests = richiesteDf
      .where(col(GseRichiestaSchema.t_stato) === STATO_NUOVO)
      .select(
        GseRichiestaSchema.n_id_gse_richiesta_er_m,
        GseRichiestaSchema.t_mese_anno,
        GseRichiestaSchema.t_cod_pod,
        GseRichiestaSchema.t_cliente
      )
      .withColumn(GseRichiestaSchema.t_mese_anno, coalesce(col(GseRichiestaSchema.t_mese_anno), lit(yearMonth)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val yearMonthList = newRequests
      .select(GseRichiestaSchema.t_mese_anno)
      .distinct()
      .map(_.getString(0))
      .collect().toList
      .flatMap(str => {
        Try(YearMonth.parse(str, DateTimeFormatter.ofPattern("MM/yyyy")))
          .toOption
          .map(ym => (ym.getYear, ym.getMonthValue))
      })

    (newRequests, yearMonthList)
  }
}