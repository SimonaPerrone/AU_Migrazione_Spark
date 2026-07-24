package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.implementation.joinintersection.JoinMeasure$Anagrafica
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{annoMeseGiornoDate, annoMeseGiornoString}
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.test_case.CreatorFactory
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.apache.spark.sql.functions.{col, lit, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, functions}

import scala.collection.mutable

class FieldCalculation4Calc1Test
  extends ForUnitTest
    with Checker {
  private val colsTipCalc1 = Array[String](AnagraficaSchema.t_codice_pdr
    , ClgPdrCapacitaSchema.n_pcm
    , ClgPdrCapacitaSchema.d_data_da
    , ClgPdrCapacitaSchema.d_data_a
  )
  private var mappingTipCalc1: mutable.Map[String, Int] = mutable.HashMap.empty
  var i = 0
  colsTipCalc1.foreach((col) => {
    mappingTipCalc1 = mappingTipCalc1 + (col -> i)
    i = i + 1
  })

  private def initInputDataset(testCase: String, one: Boolean = true)(implicit sqlContext: SQLContext,args:Args): DataFrame = {
    val in = CreatorFactory.getTestCreator(testCase)

    /** IMPORTANTE, USIAMO UN SOLO SGB */
    val addMockColF = (df: DataFrame) => {
      df.withColumn(annoMeseGiornoString, functions.concat(col( CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)))
        .withColumn(annoMeseGiornoDate, to_date(unix_timestamp(functions.concat(col( CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)), "yyyyMMdd").cast(TimestampType)))

    }
    val toReturn = if (one)
      in.getMeasures.filter(col( CalcoloConsumiSbgSchema.annomese_rif) === "202012")
    else in.getMeasures

    val measureMocked=addMockColF(toReturn)
    val input = JoinMeasure$Anagrafica.run(in.getAnagrafica,measureMocked)
    input
  }

  test("test FieldCalculation4Calc1.run with x=2 ") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 2, y = 0, automatic = false, verbose = true,executionId = "")
    val input = initInputDataset(CreatorFactory.testCase1)

    val result = FieldCalculation4Calc1.run(input).cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-30 00:00:00")
    )
    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-25 00:00:00")
    )
    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-09 00:00:00")
    )
    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-11 00:00:00")
    )
    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=3 ") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 3, y = 0, automatic = false, verbose = true,executionId = "")

    val dataset202012Test1 = initInputDataset(CreatorFactory.testCase1)

    val result = FieldCalculation4Calc1.run(dataset202012Test1)
      .cache()
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-29 00:00:00"))
    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-24 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-08 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-10 00:00:00"))

    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=4 ") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 4, y = 0, automatic = false, verbose = true,executionId = "")

    val dataset202012Test1 = initInputDataset(CreatorFactory.testCase1)

    val result = FieldCalculation4Calc1.run(dataset202012Test1)
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-28 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-23 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-07 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-04 00:00:00"))
    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with X=5 ") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 5, y = 0, automatic = false, verbose = true,executionId = "")

    val dataset202012Test1 = initInputDataset(CreatorFactory.testCase1)

    val result = FieldCalculation4Calc1.run(dataset202012Test1)
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-27 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-22 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-06 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-03 00:00:00"))
    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=10 ") {
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 10, y = 0, automatic = false, verbose = true,executionId = "")

    val dataset202012Test1 = initInputDataset(CreatorFactory.testCase1)

    val result = FieldCalculation4Calc1.run(dataset202012Test1)
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR1", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR3", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR4", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR5", false)
    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=100 ") {

    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 100, y = 0, automatic = false, verbose = true,executionId = "")
    val dataset202012Test1 = initInputDataset(CreatorFactory.testCase1)


    val result = FieldCalculation4Calc1.run(dataset202012Test1)
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR1", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR3", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR4", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR5", false)
    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=4 on more month") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 4, y = 0, automatic = false, verbose = true,executionId = "")
    val datasetDeltaY = initInputDataset(CreatorFactory.testCase7, false)

    val result = FieldCalculation4Calc1.run(datasetDeltaY)
      .cache()
    result.show(false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2021-02-25 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-11-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-11-23 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2021-02-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2021-02-07 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2021-02-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2021-02-04 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR7"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-12-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-12-04 00:00:00"))

    result.unpersist()
  }

  test("test FieldCalculation4Calc1.run with x=7 on more month") {
    var row: Array[Row] = Array()
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 7, y = 0, automatic = false, verbose = true,executionId = "")

    val datasetDeltaY = initInputDataset(CreatorFactory.testCase7, false)

    val result = FieldCalculation4Calc1.run(datasetDeltaY)
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR3", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR4", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2021-02-22 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2021-02-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2021-02-01 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR7"))
      .select(colsTipCalc1.head, colsTipCalc1.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.n_pcm)), "7.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_da)), "2020-11-07 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc1(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"))
    result.unpersist()
  }
}
