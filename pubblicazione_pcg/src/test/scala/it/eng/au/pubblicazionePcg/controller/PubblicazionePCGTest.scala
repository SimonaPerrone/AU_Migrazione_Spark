package it.eng.au.pubblicazionePcg.controller

import it.eng.au.pubblicazionePcg.dao.sbg.DailyConsumptionDAO
import it.eng.au.pubblicazionePcg.model.DailyConsumptionModel
import it.eng.au.pubblicazionePcg.schema.DailyConsumptionSchema
import it.eng.au.pubblicazionePcg.utility.SparkLocalTest
import org.apache.spark.sql.{DataFrame, SQLContext}

class PubblicazionePCGTest extends SparkLocalTest {

  def test(): Unit = {
    val df = new DailyConsumptionDAOMock().readAnnoMesePartition
    PubblicazionePCG.run(df)
  }

  class DailyConsumptionDAOMock extends DailyConsumptionDAO {
    override def readAnnoMesePartition(implicit sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._

      val sbgMisureDF = Seq(
        DailyConsumptionModel(pdr = "02800000283646", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-07-01 00:00:00.0", value = 1.2, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "02800000283646", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-07-05 00:00:00.0", value = 1.4, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "02800000283646", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-07-10 00:00:00.0", value = 1.4, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "02800000283646", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-08-20 00:00:00.0", value = 1.4, tipocliente = "U", unitmisprel = "SM3", annomese = "202108", executionid = "123"),
        DailyConsumptionModel(pdr = "02800000283646", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-08-25 00:00:00.0", value = 1.4, tipocliente = "U", unitmisprel = "SM3", annomese = "202108", executionid = "123"),

        DailyConsumptionModel(pdr = "24300000563962", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "Y", date = "2021-07-03 00:00:00.0", value = 1.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "24300000563962", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "Y", date = "2021-07-04 00:00:00.0", value = 1.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "24300000563962", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "Y", date = "2021-07-05 00:00:00.0", value = 1.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "24300000563962", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "Y", date = "2021-09-12 00:00:00.0", value = 1.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202109", executionid = "123"),
        DailyConsumptionModel(pdr = "24300000563962", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "Y", date = "2021-09-15 00:00:00.0", value = 1.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202109", executionid = "123"),

        DailyConsumptionModel(pdr = "05600000629960", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-08-04 00:00:00.0", value = 1.8, tipocliente = "U", unitmisprel = "SM3", annomese = "202108", executionid = "123"),
        DailyConsumptionModel(pdr = "05600000629960", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-08-05 00:00:00.0", value = 2.0, tipocliente = "U", unitmisprel = "SM3", annomese = "202108", executionid = "123"),
        DailyConsumptionModel(pdr = "05600000629960", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-09-05 00:00:00.0", value = 2.0, tipocliente = "U", unitmisprel = "SM3", annomese = "202109", executionid = "123"),
        DailyConsumptionModel(pdr = "05600000629960", pivait = "10238291008", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "14", codprofstd = "C2X1", treatment = "G", date = "2021-09-05 00:00:00.0", value = 2.0, tipocliente = "U", unitmisprel = "SM3", annomese = "202109", executionid = "123"),
        DailyConsumptionModel(pdr = "05600000629960", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-10-04 00:00:00.0", value = 2.2, tipocliente = "U", unitmisprel = "SM3", annomese = "202110", executionid = "123"),
        DailyConsumptionModel(pdr = "05600000629960", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-10-04 00:00:00.0", value = 2.2, tipocliente = "U", unitmisprel = "SM3", annomese = "202110", executionid = "123"),

        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-11-04 00:00:00.0", value = 2.2, tipocliente = "U", unitmisprel = "SM3", annomese = "202111", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-11-05 00:00:00.0", value = 2.4, tipocliente = "U", unitmisprel = "SM3", annomese = "202111", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-11-06 00:00:00.0", value = 2.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202111", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-07-10 00:00:00.0", value = 2.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-07-11 00:00:00.0", value = 2.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-07-12 00:00:00.0", value = 2.6, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-07-13 00:00:00.0", value = 2.8, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123"),
        DailyConsumptionModel(pdr = "15820000046655", pivait = "00866790140", pivaudd = "pivaUdd", pivardb = "10238291008", pivaudb = "pivaUdb", codremi = "14002", idregclim = "13", codprofstd = "C2X1", treatment = "G", date = "2021-07-14 00:00:00.0", value = 3.0, tipocliente = "U", unitmisprel = "SM3", annomese = "202107", executionid = "123")
      ).toDF(DailyConsumptionSchema.getValues: _*)

      sbgMisureDF
    }
  }
}
