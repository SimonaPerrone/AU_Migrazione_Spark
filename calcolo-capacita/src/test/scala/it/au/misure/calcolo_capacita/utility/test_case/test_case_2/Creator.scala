package it.au.misure.calcolo_capacita.utility.test_case.test_case_2

import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}

case class  Creator() extends it.au.misure.calcolo_capacita.utility.test_case.Creator {
 /**
  * scenario:
  * *estremi compresi
  * PDR1 (31-27) - (25-20) - (17-15) - (10-7) - 1 *tutti tranne sbg202102 e sbg202011
  * PDR1 (28-20) - (17-15) - (10-7) - 1 *sbg202102*
  * PDR1 (28-20) - (17-15) - (10-7) - 1 *sbg202011*
  * PDR2 26
  * PDR3 (23-22)  *202102*
  * PDR3 (26-22)  *202011*
  * PDR4 12 - (10-6)
  * PDR5 (12-10) - (7-1)
  * PDR7 (12-10) - (7-3) 1 *202012*
  * PDR7 (12) - (7-1) *202011*
  *
  * uguale per sbg202101, sbg202102,sbg202012,sbg202011 tranne che per PDR3,PDR7
  */
 override def getMeasures(implicit sqlContext: SQLContext): DataFrame = {
  val na=""
  import sqlContext.implicits._
  val sbg202102 = Seq(
   ("PDR1", "27", 27.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "26", 26.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "25", 25.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "28", 28.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "23", 23.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "24", 24.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "22", 22.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "21", 21.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "20", 20.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "17", 17.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "16", 16.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "15", 15.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "10", 10.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "09", 9.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "08", 8.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "07", 7.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "01", 1.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR2", "26", 26.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "23", 23.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "22", 22.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "12", 12.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "10", 10.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "09", 9.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "08", 8.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "07", 7.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "06", 6.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "12", 12.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "11", 11.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "10", 10.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "07", 7.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "06", 6.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "05", 5.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "04", 4.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "03", 3.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "02", 2.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "01", 1.0d,"202102",na,na,na,na,na,na,na,na,na,na,na,na,na)
  )
    .toDF(CalcoloConsumiSbgSchema.cod_pdr,
        CalcoloConsumiSbgSchema.giorno,
        CalcoloConsumiSbgSchema.consumo,
        CalcoloConsumiSbgSchema.annomese_rif,
        CalcoloConsumiSbgSchema.piva_it,
        CalcoloConsumiSbgSchema.piva_udd,
        CalcoloConsumiSbgSchema.piva_udb,
        CalcoloConsumiSbgSchema.piva_rdb,
        CalcoloConsumiSbgSchema.cod_remi,
        CalcoloConsumiSbgSchema.id_reg_clim,
        CalcoloConsumiSbgSchema.cod_prof_std,
        CalcoloConsumiSbgSchema.trattamento,
        CalcoloConsumiSbgSchema.trattamento_calcolo,
        CalcoloConsumiSbgSchema.tipo_cliente,
        CalcoloConsumiSbgSchema.unit_mis_prel,
        CalcoloConsumiSbgSchema.data_insert,
        CalcoloConsumiSbgSchema.sessione_sbg)

  val sbg202101 = Seq(
   ("PDR1", "31", 31.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "30", 30.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "29", 29.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "28", 28.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "27", 27.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "25", 25.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "24", 24.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "23", 23.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "22", 22.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "21", 21.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "20", 20.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "17", 17.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "16", 16.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "15", 15.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "10", 10.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "09", 9.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "08", 8.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "07", 7.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "01", 1.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR2", "26", 26.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "12", 12.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "10", 10.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "09", 9.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "08", 8.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "07", 7.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "06", 6.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "12", 12.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "11", 11.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "10", 10.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "07", 7.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "06", 6.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "05", 5.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "04", 4.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "03", 3.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "02", 2.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "01", 1.0d,"202101",na,na,na,na,na,na,na,na,na,na,na,na,na)
  )
    .toDF(CalcoloConsumiSbgSchema.cod_pdr,
        CalcoloConsumiSbgSchema.giorno,
        CalcoloConsumiSbgSchema.consumo,
        CalcoloConsumiSbgSchema.annomese_rif,
        CalcoloConsumiSbgSchema.piva_it,
        CalcoloConsumiSbgSchema.piva_udd,
        CalcoloConsumiSbgSchema.piva_udb,
        CalcoloConsumiSbgSchema.piva_rdb,
        CalcoloConsumiSbgSchema.cod_remi,
        CalcoloConsumiSbgSchema.id_reg_clim,
        CalcoloConsumiSbgSchema.cod_prof_std,
        CalcoloConsumiSbgSchema.trattamento,
        CalcoloConsumiSbgSchema.trattamento_calcolo,
        CalcoloConsumiSbgSchema.tipo_cliente,
        CalcoloConsumiSbgSchema.unit_mis_prel,
        CalcoloConsumiSbgSchema.data_insert,
        CalcoloConsumiSbgSchema.sessione_sbg)

  val sbg202012 = Seq(
   ("PDR1", "31", 31.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "30", 30.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "29", 29.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "28", 28.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "27", 27.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "25", 25.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "24", 24.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "23", 23.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "22", 22.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "21", 21.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "20", 20.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "17", 17.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "16", 16.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "15", 15.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "10", 10.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "09", 9.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "08", 8.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "07", 7.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "01", 1.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR2", "26", 26.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "12", 12.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "10", 10.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "09", 9.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "08", 8.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "07", 7.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "06", 6.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "12", 12.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "11", 11.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "10", 10.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "07", 7.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "06", 6.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "05", 5.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "04", 4.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "03", 3.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "02", 2.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "01", 1.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "12", 12.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "11", 11.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "10", 10.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "07", 7.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "06", 6.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "05", 5.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "04", 4.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "03", 3.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "01", 1.0d,"202012",na,na,na,na,na,na,na,na,na,na,na,na,na)
  )
    .toDF(CalcoloConsumiSbgSchema.cod_pdr,
        CalcoloConsumiSbgSchema.giorno,
        CalcoloConsumiSbgSchema.consumo,
        CalcoloConsumiSbgSchema.annomese_rif,
        CalcoloConsumiSbgSchema.piva_it,
        CalcoloConsumiSbgSchema.piva_udd,
        CalcoloConsumiSbgSchema.piva_udb,
        CalcoloConsumiSbgSchema.piva_rdb,
        CalcoloConsumiSbgSchema.cod_remi,
        CalcoloConsumiSbgSchema.id_reg_clim,
        CalcoloConsumiSbgSchema.cod_prof_std,
        CalcoloConsumiSbgSchema.trattamento,
        CalcoloConsumiSbgSchema.trattamento_calcolo,
        CalcoloConsumiSbgSchema.tipo_cliente,
        CalcoloConsumiSbgSchema.unit_mis_prel,
        CalcoloConsumiSbgSchema.data_insert,
        CalcoloConsumiSbgSchema.sessione_sbg)

  val sbg202011 = Seq(
   ("PDR1", "28", 28.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "27", 27.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "25", 25.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "26", 26.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "24", 24.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "23", 23.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "22", 22.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "21", 21.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "20", 20.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "17", 17.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "16", 16.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "15", 15.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "10", 10.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "09", 9.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "08", 8.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "07", 7.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR1", "01", 1.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR2", "26", 26.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "26", 26.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "25", 25.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "24", 24.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "23", 23.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR3", "22", 22.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "12", 12.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "10", 10.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "09", 9.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "08", 8.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "07", 7.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR4", "06", 6.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "12", 12.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "11", 11.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "10", 10.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "07", 7.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "06", 6.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "05", 5.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "04", 4.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "03", 3.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "02", 2.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR5", "01", 1.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "12", 12.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "07", 7.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "06", 6.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "05", 5.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "04", 4.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "03", 3.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "02", 2.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na),
   ("PDR7", "01", 1.0d,"202011",na,na,na,na,na,na,na,na,na,na,na,na,na)
  )
    .toDF(CalcoloConsumiSbgSchema.cod_pdr,
        CalcoloConsumiSbgSchema.giorno,
        CalcoloConsumiSbgSchema.consumo,
        CalcoloConsumiSbgSchema.annomese_rif,
        CalcoloConsumiSbgSchema.piva_it,
        CalcoloConsumiSbgSchema.piva_udd,
        CalcoloConsumiSbgSchema.piva_udb,
        CalcoloConsumiSbgSchema.piva_rdb,
        CalcoloConsumiSbgSchema.cod_remi,
        CalcoloConsumiSbgSchema.id_reg_clim,
        CalcoloConsumiSbgSchema.cod_prof_std,
        CalcoloConsumiSbgSchema.trattamento,
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
  import sqlContext.implicits._
   val na=""
   Seq(
     ("IDPDR1","2020-12-12 fixme",na,"PDR1",31.1,11.0,10.0,"A"),
     ("IDPDR2","2020-12-12 fixme",na,"PDR2",31.1,11.0,10.0,"A"),
     ("IDPDR3","2020-12-12 fixme",na,"PDR3",31.1,11.0,10.0,"A")
   )
    .toDF(AnagraficaSchema.getValues:_*)
    .withColumn(AnagraficaSchema.n_prelievo_annuo,col(AnagraficaSchema.n_prelievo_annuo).cast(DoubleType))
    .withColumn(AnagraficaSchema.t_pmax,col(AnagraficaSchema.t_pmax).cast(DoubleType))
    .withColumn(AnagraficaSchema.t_z,col(AnagraficaSchema.t_z).cast(DoubleType))
 }

 override def getMisureInPerimetro(implicit sqlContext: SQLContext): DataFrame = sqlContext.emptyDataFrame

 override def getRCUGasMassivo(implicit sqlContext: SQLContext): Option[DataFrame] = None
}
