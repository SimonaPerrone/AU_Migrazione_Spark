package it.au.misure.calcolo_capacita.utility.test_case.test_case_a2

import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema, PerimetroPdrPuntualeSchema, RCUGasMassivoPSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}

case class Creator() extends it.au.misure.calcolo_capacita.utility.test_case.Creator {

  override def getMeasures(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val na = ""
    val sbg202102 = Seq(
      ("PDR1", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "26", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "25", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR3", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "26", 26.0d, "202102", "M", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "25", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR4", "27", 27.0d, "202102", "M", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "26", 26.0d, "202102", "M", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "25", 25.0d, "202102", "M", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR5", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR5", "26", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR5", "25", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR6", "27", 1000.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR6", "26", 1000.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR6", "24", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR6", "21", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na)


    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr,
        CalcoloConsumiSbgSchema.giorno,
        CalcoloConsumiSbgSchema.consumo,
        CalcoloConsumiSbgSchema.annomese_rif,
        CalcoloConsumiSbgSchema.trattamento,
        CalcoloConsumiSbgSchema.piva_it,
        CalcoloConsumiSbgSchema.piva_udd,
        CalcoloConsumiSbgSchema.piva_udb,
        CalcoloConsumiSbgSchema.piva_rdb,
        CalcoloConsumiSbgSchema.cod_remi,
        CalcoloConsumiSbgSchema.id_reg_clim,
        CalcoloConsumiSbgSchema.cod_prof_std,
        CalcoloConsumiSbgSchema.trattamento_calcolo,
        CalcoloConsumiSbgSchema.tipo_cliente,
        CalcoloConsumiSbgSchema.unit_mis_prel,
        CalcoloConsumiSbgSchema.data_insert,
        CalcoloConsumiSbgSchema.sessione_sbg)

      sbg202102
  }

  override def getAnagrafica(implicit sqlContext: SQLContext): DataFrame = {
    val na=""
    import sqlContext.implicits._
    Seq(
      ("IDPDR1", "2020-12-12 fixme", na, "PDR1",  20.0,  10.0,10.0,  "A"),
      ("IDPDR2", "2020-12-12 fixme", na, "PDR2",  20.0,  10.0,10.0,  "A"),
      ("IDPDR3", "2020-12-12 fixme", na, "PDR3",  20.0,  10.0,10.0,  "A"),
      ("IDPDR4", "2020-12-12 fixme", na, "PDR4",  20.0,  10.0,10.0,  "A"),
      ("IDPDR5", "2020-12-12 fixme", na, "PDR5",  20.0,  10.0,10.0,  "A"),
      ("IDPDR6", "2020-12-12 fixme", na, "PDR6",  20.0,  10.0,10.0,  "A")

    )
      .toDF(AnagraficaSchema.getValues:_*)
      .withColumn(AnagraficaSchema.n_prelievo_annuo, col(AnagraficaSchema.n_prelievo_annuo).cast(DoubleType))
      .withColumn(AnagraficaSchema.t_pmax, col(AnagraficaSchema.t_pmax).cast(DoubleType))
      .withColumn(AnagraficaSchema.t_z, col(AnagraficaSchema.t_z).cast(DoubleType))
  }

  override def getMisureInPerimetro(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val setPdrToFilterNoFilter =
      Seq(
        ("PDR1", "N"),
        ("PDR2", "N"),
        ("PDR3", "N"),
        ("PDR4", "N"),
        ("PDR5", "N"),
        ("PDR6", "N")
      ).toDF(PerimetroPdrPuntualeSchema.t_codice_pdr, PerimetroPdrPuntualeSchema.t_valido)
    setPdrToFilterNoFilter
  }

  override def getRCUGasMassivo(implicit sqlContext: SQLContext): Option[DataFrame] = {
    import sqlContext.implicits._
    val rcugasmassivo = Seq(
      ("PDR1", "2021-02-01 00:00:00.0", "VTG"),
      ("PDR2", "2021-05-01 00:00:00.0", "VTG"),
      ("PDR3", "2021-05-01 00:00:00.0", "VTG"),
      ("PDR4", "2021-05-01 00:00:00.0", "VTG"),
      ("PDR5", "2021-05-01 00:00:00.0", "VTG"),
      ("PDR6", "2021-05-01 00:00:00.0", "VTG")

    ).toDF(RCUGasMassivoPSchema.t_codice_pdr, RCUGasMassivoPSchema.d_data_inizio_for, RCUGasMassivoPSchema.t_processo)

    Some(rcugasmassivo)

  }
}
