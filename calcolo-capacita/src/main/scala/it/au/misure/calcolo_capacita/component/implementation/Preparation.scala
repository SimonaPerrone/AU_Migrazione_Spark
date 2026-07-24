package it.au.misure.calcolo_capacita.component.implementation

import it.au.misure.calcolo_capacita.component.schema._
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.validoPdrFilterConstant
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, functions}

object Preparation {

  def selectMeasureBetweenRangeOnMeasure(measureDf: DataFrame)(implicit args: Args): DataFrame = {
    val (lowerBound, upperBound) = args.getRange()
    val listDb = Calculation.calculateRangeDb(args.dataCalc, args.y)
    val df_v2 = measureDf
      //attivo il partition pruning
      .filter(col(CalcoloConsumiSbgSchema.annomese_rif).isin(listDb: _*))

      .withColumn(annoMeseGiornoString, functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)))
      .withColumn(annoMeseGiornoDate, to_date(unix_timestamp(functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)), "yyyyMMdd").cast(TimestampType)))

    df_v2.filter(col(annoMeseGiornoDate).between(lit(lowerBound), lit(upperBound)))
  }


  def selectPdrInPerimentroOnAnagrafica(anagraficaDf: DataFrame, perimetroPdrDf: DataFrame): DataFrame = {
    val perimentroPdr_v2 = perimetroPdrDf.filter(col(PerimetroPdrPuntualeSchema.t_valido) === lit(validoPdrFilterConstant))
    val res = perimentroPdr_v2.count()
    if (res == 0) {
      LoggerUtility.printInfo("run massivo", getClass.getName)
      anagraficaDf
    } else {
      applyFilter(anagraficaDf, perimentroPdr_v2)
    }
  }

  private def applyFilter(anagrafica: DataFrame, perimetroPdr: DataFrame): DataFrame = {
    LoggerUtility.printInfo("run with set filter pdr", getClass.getName)

    val pdrToFilter2 = perimetroPdr.select(PerimetroPdrPuntualeSchema.t_codice_pdr)
      .withColumnRenamed(PerimetroPdrPuntualeSchema.t_codice_pdr, AnagraficaSchema.t_codice_pdr)
    val c: String = AnagraficaSchema.t_codice_pdr
    anagrafica.join(broadcast(pdrToFilter2), Seq(c), "inner")

  }
}
