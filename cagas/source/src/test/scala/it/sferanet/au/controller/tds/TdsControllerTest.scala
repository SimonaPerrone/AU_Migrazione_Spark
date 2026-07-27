package it.sferanet.au.controller.tds

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model._
import it.sferanet.au.schema.{PrtVsgAggRcuSchema, PrtVsgSchema, PrtVtgAggRcuSchema, PrtVtgSchema}
import it.sferanet.au.utilities.{Constants, Environment}

import java.util.Date

//@Ignore
class TdsControllerTest extends EnvironmentSparkTest {/*
  val sqlCtx = Environment.getSqlContext

  import sqlCtx.implicits._

  val sc = Environment.getSparkContext

  def testTranform(): Unit = {
    val prtVsg = Seq(
      ("pdr1", "F", "2018-04-23 00:00:00.0", "A40", "1"),
      ("pdr2", "F", "2021-08-23 00:00:00.0", "A40", "2"),
      ("pdr3", "F", "2021-08-23 00:00:00.0", "A40", "3"),
      ("pdr6", "F", "2015-08-23 00:00:00.0", "A40", "6")
    ).toDF(PrtVsgSchema.getValues: _*)

    val prtVtg = Seq(
      ("pdr3", "F3", "2021-08-23 00:00:00.0", "1"),
      ("pdr4", "F3", "2021-08-23 00:00:00.0", "2"),
      ("pdr5", "F3", "2018-04-23 00:00:00.0", "3"),
      ("pdr6", "F3", "2021-04-23 00:00:00.0", "6")
    ).toDF(PrtVtgSchema.getValues: _*)

    // Removed to "No encoders found for java.util.Date"
    /*
    val rddTds = Seq(
      ("pdr1", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo"),
      ("pdr2", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo"),
      ("pdr3", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo"),
      ("pdr4", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo"),
      ("pdr5", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo"),
      ("pdr6", "31/05/19 15:26:1559309194", true, "cat_uso", "classe_prelievo")
    ).toDF(
      "cod_pdr", "data_creazione", "valid", "cat_uso", "classe_prelievo"
    )
      .map(r => {
        val dataString = r.getAs("data_creazione").toString
        val date =
          Constants.getDate(Constants.getFormatter("dd/MM/yy HH:mm:ss"), dataString.substring(0, 17))

        Tds(
          r.getAs[Boolean]("valid"),
          r.getAs("cod_pdr").toString,
          r.getAs("cat_uso").toString,
          r.getAs("classe_prelievo").toString,
          if (date.isDefined) date.get else new Date(0)
        )
      })
      .rdd
      .map(v => (v.pdr, v)).groupByKey()
      .map({ case (pdr, values) => (pdr, values.maxBy(_.data_creazione)) })
 */
    val prtVsgAggRcu = Seq(
      ("2", "1"),
      ("1", "1"),
      ("6", "1")
    ).toDF(PrtVsgAggRcuSchema.getValues: _*)

    val prtVtgAggRcu = Seq(
      ("2", "1"),
      ("3", "1"),
      ("6", "1")
    ).toDF(PrtVtgAggRcuSchema.getValues: _*)

    val pdrsToRemove = TdsController.getPdrsToRemove(prtVsg, prtVtg, prtVsgAggRcu, prtVtgAggRcu, "2022-01-01")
    pdrsToRemove.get.collect.foreach(println)
    //TdsController.filterTds(rddTds, pdrsToRemove.get)
    //  .collect().foreach(println)
  }

  def testPrepare(): Unit = {
    val tdsEndDate = Environment.getTdsReceiveEndDate

    val prtVsg = Seq(
      ("pdr1", "1", "2018-04-23 00:00:00.0", "F", "A40"),
      ("pdr1", "1", "2020-07-23 00:00:00.0", "F", "A40"),
      ("pdr2", "2", "2021-08-23 00:00:00.0", "F", "A40")
    ).toDF(
      "t_codice_pdr", "n_id_pratica", "d_data_esecuzione", "t_stato", "t_tipo_prestazione"
    )

    TdsController.preparePrtVsg(prtVsg, tdsEndDate)
      .show()
  }*/
}
