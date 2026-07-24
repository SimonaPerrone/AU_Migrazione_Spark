package it.au.misure.calcolo_capacita.utility.test_case.test_case_a1

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
      ("PDR1", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "23", 23.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "22", 22.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "21", 21.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "20", 20.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "18", 18.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "17", 17.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "15", 15.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "07", 7.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "06", 6.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "05", 5.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "02", 2.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "01", 1.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR2", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "26", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "24", 24.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "22", 22.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "18", 18.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "17", 17.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "15", 15.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "14", 14.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "13", 13.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "12", 12.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "11", 11.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "10", 10.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "05", 5.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "04", 4.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "02", 2.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "01", 1.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR3", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "26", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "25", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "24", 24.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "23", 23.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR3", "22", 22.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDR4", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "27", 27.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "26", 26.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "25", 25.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "24", 24.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "23", 23.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "22", 22.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "21", 21.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "20", 20.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "19", 19.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "18", 18.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "17", 17.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "16", 16.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "15", 15.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "14", 14.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "13", 13.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "12", 12.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "11", 11.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "10", 10.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "09", 9.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "08", 8.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "07", 7.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "06", 6.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "05", 5.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "04", 4.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "03", 3.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "02", 2.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "01", 1.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDR5", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDRM", "28", 28.0d, "202102", "Z", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDRM", "28", 28.0d, "202102", "Z", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDRM", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDRF", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDRF", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDRF", "28", 28.0d, "202102", "A", na, na, na, na, na, na, na, na, na, na, na, na)

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

    val sbg202101 = Seq(
      ("PDR1", "31", 31.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "30", 30.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "29", 29.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "28", 28.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDR2", "26", 26.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "10", 10.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "09", 9.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "08", 8.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "07", 7.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "06", 6.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "05", 5.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "04", 4.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "03", 3.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "02", 2.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "01", 1.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR4", "31", 31.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "30", 30.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "29", 29.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "28", 28.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "27", 27.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "26", 26.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "25", 25.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "24", 24.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "23", 23.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "22", 22.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "21", 21.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "20", 20.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "19", 19.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "18", 18.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "17", 17.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "16", 16.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "15", 15.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "14", 14.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "13", 13.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "12", 12.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "11", 11.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "10", 10.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "09", 9.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "08", 8.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "07", 7.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "06", 6.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "05", 5.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "04", 4.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "03", 3.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "02", 2.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR4", "01", 1.0d, "202101", "A", na, na, na, na, na, na, na, na, na, na, na, na)
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno, CalcoloConsumiSbgSchema.consumo, CalcoloConsumiSbgSchema.annomese_rif, CalcoloConsumiSbgSchema.trattamento,
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

    val sbg202012 = Seq(
      ("PDR1", "31", 31.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "30", 30.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "29", 29.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "28", 28.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),


      ("PDR2", "31", 31.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "30", 30.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "29", 29.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "28", 28.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "28", 28.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "27", 27.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "26", 26.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "10", 10.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "09", 9.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "08", 8.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "07", 7.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "06", 6.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "05", 5.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "04", 4.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "03", 3.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "02", 2.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "01", 1.0d, "202012", "A", na, na, na, na, na, na, na, na, na, na, na, na)
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno, CalcoloConsumiSbgSchema.consumo, CalcoloConsumiSbgSchema.annomese_rif, CalcoloConsumiSbgSchema.trattamento,
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

    val sbg202011 = Seq(
      ("PDR1", "28", 28.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "27", 27.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "25", 25.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "26", 26.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "24", 24.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "23", 23.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "22", 22.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "21", 21.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "20", 20.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "19", 19.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "18", 18.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "17", 17.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "16", 16.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "15", 15.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "14", 14.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "13", 13.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "12", 12.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR1", "11", 11.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),

      ("PDR2", "16", 16.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "15", 15.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "14", 14.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "13", 13.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "12", 12.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "11", 11.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "10", 10.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "09", 9.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "08", 8.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "07", 7.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "06", 6.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "05", 5.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "04", 4.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "03", 3.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "02", 2.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na),
      ("PDR2", "01", 1.0d, "202011", "A", na, na, na, na, na, na, na, na, na, na, na, na)
    )
      .toDF(CalcoloConsumiSbgSchema.cod_pdr, CalcoloConsumiSbgSchema.giorno, CalcoloConsumiSbgSchema.consumo, CalcoloConsumiSbgSchema.annomese_rif, CalcoloConsumiSbgSchema.trattamento,
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

    sbg202101 unionAll
      sbg202102 unionAll
      sbg202012 unionAll
      sbg202011
  }

  override def getAnagrafica(implicit sqlContext: SQLContext): DataFrame = {
    val na=""
    import sqlContext.implicits._
    Seq(
      ("IDPDR1", "2020-12-12 fixme", na, "PDR1",  31.1,  11.0,10.0,  "A"),
      ("IDPDR2", "2020-12-12 fixme", na, "PDR2",  30.0,  10.0,9.0,   "A"),
      ("IDPDR4", "2020-12-12 fixme", na, "PDR4",  10.0,  10.0,10.0,  "A"),
      ("IDPDR5", "2020-12-12 fixme", na, "PDR5",  1.0,   1.0, 1.0,  "A"),
      ("IDPDRX", "2020-12-12 fixme", na, "PDRX",  10.0,  10.0,10.0,  "A"),
      ("IDPDRF", "2020-12-12 fixme", na, "PDRF",  30.0,  10.0,9.0,   "A"),
      ("IDPDRM", "2020-12-12 fixme", na, "PDRM",  30.0,  10.0,9.0,   "A")

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
        ("PDR4", "N"),
        ("PDR5", "N"),
        ("PDRM", "N"),
        ("PDRF", "N")
      ).toDF(PerimetroPdrPuntualeSchema.t_codice_pdr, PerimetroPdrPuntualeSchema.t_valido)
    setPdrToFilterNoFilter
  }

  override def getRCUGasMassivo(implicit sqlContext: SQLContext): Option[DataFrame] = {
    import sqlContext.implicits._
    val rcugasmassivo = Seq(
      ("PDR1", "2021-04-12 00:00:00.0", "VTG"),
      ("PDRX", "2021-04-12 00:00:00.0", "VTG"),
      ("PDR2", "2021-04-12 00:00:00.0", "VSG"),
      ("PDR3", "2021-04-12 00:00:00.0", "VA"),
      ("PDR4", "2021-04-12 00:00:00.0", "VA"),
      ("PDR5", "2021-04-12 00:00:00.0", "VA"),
      ("PDR6", "2021-04-12 00:00:00.0", "VA"),
      ("PDRF", "2021-02-12 00:00:00.0", "VA"),
      ("PDRM", "2021-04-12 00:00:00.0", "VA")

    ).toDF(RCUGasMassivoPSchema.t_codice_pdr, RCUGasMassivoPSchema.d_data_inizio_for, RCUGasMassivoPSchema.t_processo)

    Some(rcugasmassivo)

  }
}
