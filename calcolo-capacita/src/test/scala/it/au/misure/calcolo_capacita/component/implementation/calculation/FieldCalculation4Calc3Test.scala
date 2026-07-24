package it.au.misure.calcolo_capacita.component.implementation.calculation

import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.test_case.CreatorFactory
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}

import scala.collection.mutable

class FieldCalculation4Calc3Test
  extends ForUnitTest
    with Checker {
  private val colsTipCalc3 = Array[String](AnagraficaSchema.t_codice_pdr
    , ClgPdrCapacitaSchema.n_pcm
    , ClgPdrCapacitaSchema.d_data_da
    , ClgPdrCapacitaSchema.d_data_a
  )
  private var mappingCalc3: mutable.Map[String, Int] = mutable.HashMap.empty
  var i = 0
  colsTipCalc3.foreach((col) => {
    mappingCalc3 = mappingCalc3 + (col -> i)
    i = i + 1
  })

  test("test FieldCalculation4Calc3.run generic ") {
    var row: Array[Row] = Array()
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCase1)

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    val y = 10
    implicit val args = Args(dataCalc = dataCalc, x = 0, y = y, automatic = false, verbose = true,executionId = "")

    val result = FieldCalculation4Calc3.run(in.getAnagrafica)
      .cache()
    result.show(false)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(colsTipCalc3.head, colsTipCalc3.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_a)), "2021-02-19 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR2"))
      .select(colsTipCalc3.head, colsTipCalc3.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.n_pcm)), "300.0"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_a)), "2021-02-19 00:00:00"))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(colsTipCalc3.head, colsTipCalc3.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.n_pcm)), "290.0"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mappingCalc3(ClgPdrCapacitaSchema.d_data_a)), "2021-02-19 00:00:00"))
    result.unpersist()
  }
}
