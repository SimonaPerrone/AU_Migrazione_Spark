package it.eng.au.ERP.trasformations.INT

import it.eng.au.ERP.schema.au.flussoMisureInterconnessioneSchema
import it.eng.au.ERP.schema.erp.{erpDettaglioINTSchema, erpValidatedIntSchema}
import it.eng.au.ERP.schema.rcu.{RcuPodInterconnesionePSchema, rcuPodPSchema}
import org.apache.spark.sql.functions.{col, udf, regexp_replace}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object CalcoloPrelevatoPuntiPrelievoOrariINT {

  def dfFlussoMisureInterconnessioneFilteredArea(dsFlussoMisureInterconnessioneFilteredTimestampMax: Dataset[Row],
                                                 area: Option[String])
                                                (implicit spark: SparkSession): DataFrame = {


    val dsFilterArea = area match {
      case Some(areaValue) => dsFlussoMisureInterconnessioneFilteredTimestampMax
        .filter(col(RcuPodInterconnesionePSchema.t_area_rif) === areaValue)
      case None => dsFlussoMisureInterconnessioneFilteredTimestampMax
    }


    dsFilterArea.toDF
  }

  // Define your UDF
  val funzionePivaCasi = udf((pivagdrmis: String, pivagdrinst: String, pivagdralt: String) => {
    // Example logic - replace with your own
    if (pivagdrmis != pivagdrinst && pivagdrmis != pivagdralt && pivagdrinst != pivagdralt) {
      1
    } else if (pivagdrmis == pivagdrinst && pivagdrmis != pivagdralt) {
      2
    } else if (pivagdrmis == pivagdralt && pivagdrinst != pivagdrmis) {
      3
    }
    else if (pivagdrinst == pivagdralt && pivagdrinst != pivagdrmis) {
      4
    }
    else if (pivagdrmis == pivagdrinst && pivagdrmis == pivagdralt) {
      5
    }
    else {
      0
    }
  })


  private val energiaPrelievoColumns: Seq[String] =
    sortByNumericSuffix(erpValidatedIntSchema.getValues, "ea_e")
  private val energiaImmissioneColumns: Seq[String] =
    sortByNumericSuffix(erpValidatedIntSchema.getValues, "eaint_e")
  private val quartColumns: Seq[String] =
    sortByNumericSuffix(erpDettaglioINTSchema.getValues, "q")

  private val quartileTriplets: Seq[(String, String, String)] =
    quartColumns.zip(energiaPrelievoColumns).zip(energiaImmissioneColumns).map {
      case ((quart, prelievo), immissione) => (quart, prelievo, immissione)
    }

  // Normalize numeric strings with comma decimal to Double
  private def toDouble(c: Column): Column = {
    regexp_replace(c.cast("string"), ",", ".").cast(DoubleType)
  }

  private def sortByNumericSuffix(values: Seq[String], prefix: String): Seq[String] =
    values.flatMap { value =>
      if (value.startsWith(prefix)) {
        val suffix = value.substring(prefix.length)
        if (suffix.nonEmpty && suffix.forall(_.isDigit)) {
          Some((suffix.toInt, value))
        } else {
          None
        }
      } else {
        None
      }
    }.sortBy(_._1).map(_._2)

  private def multiplyColumns(df: DataFrame, columns: Seq[String], multiplier: Column): DataFrame =
    // IMPORTANT: EA/EAINT columns often arrive as strings with comma decimal separator (e.g. "159,65").
    // Using plain cast(DoubleType) would yield NULL. Always normalize with toDouble before multiplying.
    columns.foldLeft(df) { (acc, columnName) =>
      acc.withColumn(columnName, toDouble(col(columnName)) * multiplier)
    }


  def dfCurvaPrelievo(dfErpDettaglioInt: DataFrame)
                     (implicit spark: SparkSession): DataFrame = {
    val dfErpDettaglioIntCast = dfErpDettaglioInt
      .withColumn(erpValidatedIntSchema.coefficienteperditaprel, toDouble(col(erpValidatedIntSchema.coefficienteperditaprel)))
      .withColumn(erpValidatedIntSchema.ka, toDouble(col(erpValidatedIntSchema.ka)))

    val multiplier = col(erpValidatedIntSchema.ka) * col(erpValidatedIntSchema.coefficienteperditaprel)
    multiplyColumns(dfErpDettaglioIntCast, energiaPrelievoColumns, multiplier)
  }



  def dfCurvaImmissione(dfErpDettaglioInt: DataFrame)
                        (implicit spark: SparkSession): DataFrame = {
    val dfErpDettaglioIntCast = dfErpDettaglioInt
      .withColumn(erpValidatedIntSchema.coefficienteperditaimm, toDouble(col(erpValidatedIntSchema.coefficienteperditaimm)))
      .withColumn(erpValidatedIntSchema.ka, toDouble(col(erpValidatedIntSchema.ka)))

    val multiplier = col(erpValidatedIntSchema.ka) * col(erpValidatedIntSchema.coefficienteperditaimm)
    multiplyColumns(dfErpDettaglioIntCast, energiaImmissioneColumns, multiplier)
  }



  def dfCampioneDiff1(dfErpDettaglioInt: DataFrame)
                      (implicit spark: SparkSession): DataFrame = {

    quartileTriplets.foldLeft(dfErpDettaglioInt) {
      case (acc, (quartCol, prelievoCol, immissioneCol)) =>
        acc.withColumn(quartCol, toDouble(col(prelievoCol)) - toDouble(col(immissioneCol)))
    }
  }



  def dfCampioneDiff2(dfErpDettaglioInt: DataFrame)
                      (implicit spark: SparkSession): DataFrame = {

    quartileTriplets.foldLeft(dfErpDettaglioInt) {
      case (acc, (quartCol, prelievoCol, immissioneCol)) =>
        acc.withColumn(quartCol, toDouble(col(immissioneCol)) - toDouble(col(prelievoCol)))
    }

  }


  def dfErpValidatoIntFilteredPiva(dfErpValidatoInt: DataFrame,
                                   singola_piva_distributore: Option[String])
                                  (implicit spark: SparkSession): DataFrame = {


    val dsFilterPiva = singola_piva_distributore match {
      case Some(singola_piva_distributoreValue) => dfErpValidatoInt
        .filter(col(erpValidatedIntSchema.pivagdrinst) === singola_piva_distributoreValue ||
          col(erpValidatedIntSchema.pivagdrmis) === singola_piva_distributoreValue ||
          col(erpValidatedIntSchema.pivagdralt) === singola_piva_distributoreValue
        )
      case None => dfErpValidatoInt
    }


    dsFilterPiva.toDF
  }


  def dfErpValidatoIntFilteredAnnoMese(dsFlussoMisureInterconnessione: Dataset[Row],
                                       year: Option[Int],
                                       month: Option[Int]
                                      )
                                      (implicit spark: SparkSession): DataFrame = {

    val dfFilteredYear = year match {
      case Some(yearValue) => dsFlussoMisureInterconnessione
        .filter(col(flussoMisureInterconnessioneSchema.anno).cast(IntegerType) === yearValue)
      case None => dsFlussoMisureInterconnessione
    }

    val dfFilteredMonth = month match {
      case Some(monthValue) => dfFilteredYear
        .filter(col(flussoMisureInterconnessioneSchema.mese).cast(IntegerType) === monthValue)
      case None => dfFilteredYear
    }


    dfFilteredMonth
  }

}
