package it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation

import it.au.misure.calcolo_capacita.component.implementation.joinintersection.JoinAnagrafica$RcuGasMassivo
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, RCUGasMassivoPSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{PATH$KO, PATH$OK}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField._
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate.ConvertString
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row}

class FieldCalculationContinuitaFornituraTest
  extends ForUnitTest
    with Checker {


  private def check(df: DataFrame, pdrCol: String, pdrValue: String, flag: String, valueOfFlag: String): Unit = {
    var row: Array[Row] = Array()
    row = df.filter(col(pdrCol) === lit(pdrValue))
      .select(flag).take(1)
    checksValues(Tuple2(getValueCalculatedOpti(row, 0), valueOfFlag))
  }

  test("test FieldCalculationContinuitaFornitura.run") {
    import sqlContext.implicits._
    val na = ""
    val anagrafica =
      Seq(
        ("IDPDR1", "2020-12-12 fixme", na, "PDR1", 31.1, 11.0, 10.0, "A"),
        ("IDPDR2", "2020-12-12 fixme", na, "PDR2", 30.0, 10.0, 9.0, "A"),
        ("IDPDR4", "2020-12-12 fixme", na, "PDR4", 10.0, 10.0, 10.0, "A"),
        ("IDPDR5", "2020-12-12 fixme", na, "PDR5", 1.0, 1.0, 1.0, "A"),
        ("IDPDRX", "2020-12-12 fixme", na, "PDRX", 10.0, 10.0, 10.0, "A"),
        ("IDPDRF", "2020-12-12 fixme", na, "PDRF", 30.0, 10.0, 9.0, "A"),
        ("IDPDRM", "2020-12-12 fixme", na, "PDRM", 30.0, 10.0, 9.0, "A")
      )
        .toDF(AnagraficaSchema.getValues: _*)
    val rcugasmassivo = Seq(
      ("PDR1", "2021-02-12 00:00:00.0", "VTG"),
      ("PDR2", "2021-01-28 00:00:00.0", "VSG"),
      ("PDR3", "2021-03-12 00:00:00.0", "VA"))
      .toDF(RCUGasMassivoPSchema.t_codice_pdr,
        RCUGasMassivoPSchema.d_data_inizio_for,
        RCUGasMassivoPSchema.t_processo)

    val dataCalc = "2021/02/01".getLocalDate("yyyy/MM/dd")
    val y = 30
    implicit val args = Args(dataCalc = dataCalc, x = 0, y = y, automatic = false, verbose = true,executionId = "")

    val anagrafica_v2 = JoinAnagrafica$RcuGasMassivo.run(anagrafica, rcugasmassivo)
    val result = FieldCalculationContinuitaFornitura.run(anagrafica_v2)
      .cache()
    result.show(1000, false)

    check(result, AnagraficaSchema.t_codice_pdr, "PDR1", PATH_CHECK_FORNITURA, PATH$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR2", PATH_CHECK_FORNITURA, PATH$KO)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR4", PATH_CHECK_FORNITURA, PATH$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDR5", PATH_CHECK_FORNITURA, PATH$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDRM", PATH_CHECK_FORNITURA, PATH$OK)
    check(result, AnagraficaSchema.t_codice_pdr, "PDRF", PATH_CHECK_FORNITURA, PATH$OK)


    result.unpersist()
  }


}
