package it.eng.au.gse.calcoloAnnuale.controller

import it.eng.au.gse.calcoloAnnuale.schema.GseRichiestaSchema
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.dao.GsePerimetroDao.t_cliente
import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.Constants._
import it.eng.au.gse.common.utility.Properties
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel

import java.time.YearMonth
import scala.util.Try

object Prepare {
  def preparePodPerimeter(perimetroDf: DataFrame): DataFrame = {
    perimetroDf
      .where(col(GsePerimetroSchema.t_valido) === STATO_VALIDO)
      .withColumn(GsePerimetroDao.t_anno, from_unixtime(unix_timestamp(col(GsePerimetroSchema.t_mese_anno), "mm/YYYY"), "YYYY"))
      .select(
        GsePerimetroSchema.t_cod_pod,
        GsePerimetroSchema.t_mese_anno,
        GsePerimetroDao.t_anno
      )
      .distinct()
  }

  def prepareRequests(richiesteDf: DataFrame): (DataFrame, List[(Int, Int)]) = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val currentYear = Properties.getYear

    val newRequests = richiesteDf
      .where(col(GseRichiestaSchema.t_stato) === STATO_NUOVO)
      .select(
        GseRichiestaSchema.n_id_gse_richiesta_er_a,
        GseRichiestaSchema.t_anno
      )
      .withColumn(GseRichiestaSchema.t_anno, coalesce(col(GseRichiestaSchema.t_anno), lit(currentYear)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val yearMonthList = newRequests
      .select(GseRichiestaSchema.t_anno)
      .distinct()
      .map(_.getString(0))
      .flatMap(s => Try(s.toInt).toOption)
      .collect().toList
      .flatMap(year => {
        if (year == currentYear.toInt) (1 to 12).flatMap(i => Try(YearMonth.now().minusMonths(i)).toOption)
        else if (year < currentYear.toInt) (1 to 12).flatMap(month => Try(YearMonth.of(year, month)).toOption)
        else None
      })
      .distinct
      .map(ym => (ym.getYear, ym.getMonthValue))

    (newRequests, yearMonthList)
  }
}
