package it.au.misure.calcolo_capacita.utility.test_case.test_case_4

import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}

case class Creator() extends it.au.misure.calcolo_capacita.utility.test_case.Creator {

  override def getMeasures(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val sbg202102 = Seq(
      ("PDR1", "27", 27.0d, "202102"),
      ("PDR1", "26", 26.0d, "202102"),
      ("PDR1", "25", 25.0d, "202102"),
      ("PDR1", "28", 28.0d, "202102"),
      ("PDR1", "23", 23.0d, "202102"),
      ("PDR1", "22", 22.0d, "202102"),
      ("PDR1", "21", 21.0d, "202102"),
      ("PDR1", "20", 20.0d, "202102"),
      ("PDR1", "18", 18.0d, "202102"),
      ("PDR1", "19", 18.0d, "202102"),
      ("PDR1", "17", 17.0d, "202102"),
      ("PDR1", "15", 15.0d, "202102"),
      ("PDR1", "07", 7.0d, "202102"),
      ("PDR1", "06", 6.0d, "202102"),
      ("PDR1", "05", 5.0d, "202102"),
      ("PDR1", "02", 2.0d, "202102"),
      ("PDR1", "01", 1.0d, "202102"),

      ("PDR2", "26", 26.0d, "202102")
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno,  CalcoloConsumiSbgSchema.consumo,  CalcoloConsumiSbgSchema.annomese_rif)

    val sbg202101 = Seq(
      ("PDR1", "31", 31.0d, "202101"),
      ("PDR1", "30", 30.0d, "202101"),
      ("PDR1", "29", 29.0d, "202101"),
      ("PDR1", "28", 28.0d, "202101"),
      ("PDR2", "26", 26.0d, "202101")
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno,  CalcoloConsumiSbgSchema.consumo,  CalcoloConsumiSbgSchema.annomese_rif)

    val sbg202012 = Seq(
      ("PDR1", "31", 31.0d, "202012"),
      ("PDR1", "30", 30.0d, "202012"),
      ("PDR1", "29", 29.0d, "202012"),
      ("PDR1", "28", 28.0d, "202012"),
      ("PDR2", "26", 26.0d, "202012")
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno,  CalcoloConsumiSbgSchema.consumo,  CalcoloConsumiSbgSchema.annomese_rif)

    val sbg202011 = Seq(
      ("PDR1", "28", 28.0d, "202011"),
      ("PDR1", "27", 27.0d, "202011"),
      ("PDR1", "25", 25.0d, "202011"),
      ("PDR1", "26", 26.0d, "202011"),
      ("PDR1", "24", 24.0d, "202011"),
      ("PDR1", "23", 23.0d, "202011"),
      ("PDR1", "22", 22.0d, "202011"),
      ("PDR1", "21", 21.0d, "202011"),
      ("PDR1", "20", 20.0d, "202011"),
      ("PDR1", "19", 19.0d, "202011"),
      ("PDR1", "18", 18.0d, "202011"),
      ("PDR1", "17", 17.0d, "202011"),
      ("PDR1", "16", 16.0d, "202011"),
      ("PDR1", "15", 15.0d, "202011"),
      ("PDR1", "14", 14.0d, "202011"),
      ("PDR1", "13", 13.0d, "202011"),
      ("PDR1", "12", 12.0d, "202011"),
      ("PDR1", "11", 11.0d, "202011"),
      ("PDR2", "26", 26.0d, "202011")
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno,  CalcoloConsumiSbgSchema.consumo,  CalcoloConsumiSbgSchema.annomese_rif)


    sbg202101 unionAll
      sbg202102 unionAll
      sbg202012 unionAll
      sbg202011
  }

  override def getAnagrafica(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    Seq(
      ("IDPDR1", "PDR1", "2020-12-12 fixme", 31.1, 10.0, 11.0)
    )
      .toDF(AnagraficaSchema.n_id_pdr, AnagraficaSchema.t_codice_pdr, "data_calc", AnagraficaSchema.n_prelievo_annuo, AnagraficaSchema.t_pmax, AnagraficaSchema.t_z)
      .withColumn(AnagraficaSchema.n_prelievo_annuo, col(AnagraficaSchema.n_prelievo_annuo).cast(DoubleType))
      .withColumn(AnagraficaSchema.t_pmax, col(AnagraficaSchema.t_pmax).cast(DoubleType))
      .withColumn(AnagraficaSchema.t_z, col(AnagraficaSchema.t_z).cast(DoubleType))
  }

  override def getMisureInPerimetro(implicit sqlContext: SQLContext): DataFrame = sqlContext.emptyDataFrame

  override def getRCUGasMassivo(implicit sqlContext: SQLContext): Option[DataFrame] = None
}
