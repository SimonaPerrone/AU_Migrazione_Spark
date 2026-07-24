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
import org.apache.spark.sql.{DataFrame, Row, functions}

import scala.collection.mutable

class FieldCalculation4Calc2Test
  extends ForUnitTest
    with Checker {

  private val colsTipCalc2 = Array[String](AnagraficaSchema.t_codice_pdr
    , ClgPdrCapacitaSchema.n_pcm
    , ClgPdrCapacitaSchema.d_data_da
    , ClgPdrCapacitaSchema.d_data_a
  )
  private var mappingTipCalc2: mutable.Map[String, Int] = mutable.HashMap.empty
  var i = 0
  colsTipCalc2.foreach((col) => {
    mappingTipCalc2 = mappingTipCalc2 + (col -> i)
    i = i + 1
  })

  private def initDataset(dataFrame: DataFrame, anagrafica: DataFrame)(implicit args: Args): DataFrame = {

    val addMockColF = (df: DataFrame) => {
      df.withColumn(annoMeseGiornoString, functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)))
        .withColumn(annoMeseGiornoDate, to_date(unix_timestamp(functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)), "yyyyMMdd").cast(TimestampType)))
    }
    val toReturn = addMockColF(dataFrame)
    val measureMocked = addMockColF(toReturn)
    val input = JoinMeasure$Anagrafica.run(anagrafica, measureMocked)
    input.withColumnRenamed(AnagraficaSchema.t_codice_pdr,AnagraficaSchema.t_codice_pdr)
  }

  test("test FieldCalculation4Calc2.run generic 1") {
    var row: Array[Row] = Array()
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCase1)

    var misureDeltaY = in.getMeasures
    val anagrafica = in.getAnagrafica
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 0, y = 0, automatic = false, verbose = true,executionId = "")

    misureDeltaY = misureDeltaY
      .filter((col(CalcoloConsumiSbgSchema.cod_pdr) !== lit("PDR1")) and
        (col(CalcoloConsumiSbgSchema.cod_pdr) !== lit("PDR2")))

    val result = FieldCalculation4Calc2.run(initDataset(misureDeltaY, anagrafica))
      .cache()

    result.show(false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR1", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR2", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-22 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-06 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"))
    result.unpersist()
  }

  test("test FieldCalculation4Calc2.run generic 2") {
    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 0, y = 0, automatic = false, verbose = true,executionId = "")

    var row: Array[Row] = Array()
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCase7)

    var datasetDeltaY = in.getMeasures(sqlContext)
    datasetDeltaY = datasetDeltaY
      .filter((col(CalcoloConsumiSbgSchema.cod_pdr) !== lit("PDR1")) and
        (col(CalcoloConsumiSbgSchema.cod_pdr) !== lit("PDR5")))

    val result = FieldCalculation4Calc2.run(initDataset(datasetDeltaY,in.getAnagrafica))
      .cache()

    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR1", false)
    checkIfExistsPdr(result, AnagraficaSchema.t_codice_pdr, "PDR5", false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR2"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-26 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-23 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-22 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-06 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR7"))
      .select(colsTipCalc2.head, colsTipCalc2.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_da)), "2020-12-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingTipCalc2(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"))
    result.unpersist()
  }
}
