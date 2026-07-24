package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.implementation.calculation.{FieldCalculation4Calc2, FieldCalculation4Calc3}
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, CalcoloConsumiSbgSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{typeCalc2Value, typeCalc3Value}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.{annoMeseGiornoDate, annoMeseGiornoString}
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.test_case.CreatorFactory
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.apache.spark.sql.functions.{col, lit, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, functions}

import scala.collection.mutable

class JoinTipCalc2$TipCalc3Test
  extends ForUnitTest
    with Checker {

  private val cols = Array[String](AnagraficaSchema.t_codice_pdr
    , ClgPdrCapacitaSchema.n_pcm
    , ClgPdrCapacitaSchema.d_data_da
    , ClgPdrCapacitaSchema.d_data_a
    , ClgPdrCapacitaSchema.t_tipo_calcolo
  )
  private var mapping: mutable.Map[String, Int] = mutable.HashMap.empty
  var i = 0
  cols.foreach((col) => {
    mapping = mapping + (col -> i)
    i = i + 1
  })

  private def initDataset(dataFrame: DataFrame): DataFrame = {
    val addMockColF = (df: DataFrame) => {
      df.withColumn(annoMeseGiornoString, functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)))
        .withColumn(annoMeseGiornoDate, to_date(unix_timestamp(functions.concat(col(CalcoloConsumiSbgSchema.annomese_rif), col(CalcoloConsumiSbgSchema.giorno)), "yyyyMMdd").cast(TimestampType)))
    }
    addMockColF(dataFrame)

  }

  test("test JoinTipCalc2$TipCalc3.run ") {
    var row: Array[Row] = Array()
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCase5)

    implicit val args = Args(dataCalc = "2020/11/11".getLocalDate("yyyy/MM/dd"), x = 0, y = 0, automatic = false, verbose = true,executionId = "")

    val join = JoinMeasure$Anagrafica.run(in.getAnagrafica, initDataset(in.getMeasures))
    val result2 = FieldCalculation4Calc2.run(join)
      .cache()
    val result3 = FieldCalculation4Calc3.run(in.getAnagrafica)
      .cache()


    val result = JoinTipCalc2$TipCalc3.run(result2, result3)

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-10-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))

    row = result.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "300.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-10-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))


    row = result2.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR3"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "26.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-26 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2020-11-22 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value))


    row = result2.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2020-11-06 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value))


    row = result2.filter(col(AnagraficaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "12.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2020-11-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value))

    result2.unpersist()
  }

}
