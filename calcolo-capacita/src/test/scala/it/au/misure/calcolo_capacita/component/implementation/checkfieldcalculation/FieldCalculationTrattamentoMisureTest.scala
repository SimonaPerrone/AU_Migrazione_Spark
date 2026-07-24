package it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation

import it.au.misure.calcolo_capacita.component.implementation.joinintersection.JoinMeasure$Anagrafica
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{PATH$OK$PRESENTI$TOT$GR$Z$GR$0, PATH$OK$PRESENTI$Z$EQ$0$OK, PATH$OK$PRESENTI$Z$EQ$TOT$OK}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate.ConvertString
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.apache.spark.sql.functions.{col, lit, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, functions}

class FieldCalculationTrattamentoMisureTest
  extends ForUnitTest
    with Checker {


  private def check(df: DataFrame, pdrCol: String, pdrValue: String, flag: String, valueOfFlag: String): Unit = {
    var row: Array[Row] = Array()
    row = df.filter(col(pdrCol) === lit(pdrValue))
      .select(flag).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, 0), valueOfFlag))
  }

  test("test FieldCalculationTrattamentoMisure.run " +
    "PDR1 con 0 misure con misure.trattamento in (Y,M) => PATH=ZEQ0" +
    "PDR2 (tot misure=2) con una misura  con misure.trattamento in (Y,M) => PATH=TOTGRZGR0" +
    "PDR3 con tutte le misure  con misure.trattamento in (Y,M) => PATH=ZEQTOT" +
    "PDR4 con 0 misure con misure.trattamento in (Y,M) => PATH=ZEQ0" +
    "PDR5 con 0 misure con misure.trattamento in (Y,M) => PATH=ZEQ0" +
    "PDR6 (tot misure=2) con 1 misure con misure.trattamento in (Y,M) => PATH=TOTGRZGR0"

  )
  {
    val na=""
    import sqlContext.implicits._
    val misure = Seq(
      ("PDR1", "28", 28.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "27", 27.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "25", 25.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "24", 24.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "23", 23.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "22", 22.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "21", 21.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "20", 20.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "17", 17.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "16", 16.0d, "202102", "B",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "15", 15.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "10", 10.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "09", 9.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "08", 8.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "07", 7.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR1", "01", 1.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR2", "26", 26.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR2", "25", 26.0d, "202102", "Q",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR3", "25", 25.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR3", "26", 26.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR3", "24", 24.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR3", "23", 23.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR3", "22", 22.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "12", 12.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "10", 10.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "09", 9.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "08", 8.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "07", 7.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR4", "06", 6.0d, "202102", "Z",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "12", 12.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "11", 11.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "10", 10.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "07", 7.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "06", 6.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "05", 5.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "04", 4.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "03", 3.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "02", 2.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR5", "01", 1.0d, "202102", "E",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR6", "12", 12.0d, "202102", "Y",na,na,na,na,na,na,na,na,na,na,na,na,na),
      ("PDR6", "11", 11.0d, "202102", "A",na,na,na,na,na,na,na,na,na,na,na,na,na)
    )
      .toDF(	CalcoloConsumiSbgSchema.cod_pdr,
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
        CalcoloConsumiSbgSchema.sessione_sbg,
        PATH_CHECK_FORNITURA
      )
      .withColumn(annoMeseGiornoString, functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)))
      .withColumn(annoMeseGiornoDate, to_date(unix_timestamp(functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)), "yyyyMMdd").cast(TimestampType)))

    val anagraficaDf =
      Seq(
        ("IDPDR1", "2020-12-12 fixme", na, "PDR1", 31.1, 11.0, 10.0, "A",na),
        ("IDPDR2", "2020-12-12 fixme", na, "PDR2", 30.0, 10.0, 9.0, "A",na),
        ("IDPDR3", "2020-12-12 fixme", na, "PDR3", 10.0, 10.0, 10.0, "A",na),
        ("IDPDR4", "2020-12-12 fixme", na, "PDR4", 10.0, 10.0, 10.0, "A",na),
        ("IDPDR5", "2020-12-12 fixme", na, "PDR5", 10.0, 10.0, 10.0, "A",na),
        ("IDPDR6", "2020-12-12 fixme", na, "PDR6", 10.0, 10.0, 10.0, "A",na)
      )
        .toDF(AnagraficaSchema.getValues:::List(PATH_CHECK_FORNITURA) : _*)
    val dataCalc = "2021/01/02".getLocalDate("yyyy/MM/dd")
    val y = 30
    implicit val args = Args(dataCalc = dataCalc, x = 0, y = y, automatic = false, verbose = true,executionId = "")

    val join= JoinMeasure$Anagrafica.run(anagraficaDf, misure)
    val result = FieldCalculationTrattamentoMisure.run(join)
      .cache()

    result.show(1000, false)

    check(result, AnagraficaSchema.t_codice_pdr, "PDR1", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$Z$EQ$0$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR2", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$TOT$GR$Z$GR$0)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR3", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$Z$EQ$TOT$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR4", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$Z$EQ$0$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR5", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$Z$EQ$0$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR6", PATH_CHECK_TRATTAMENTO, PATH$OK$PRESENTI$TOT$GR$Z$GR$0)


    result.unpersist()
  }

  }
