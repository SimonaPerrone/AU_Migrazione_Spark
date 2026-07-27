package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema.{CaPreFinalSchema, CaSchema, CodProfStdDaTdsSchema, PdrMassivoSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.junit.Assert

class FilterPerimetroAggRicTest extends EnvironmentSparkTest {

  val pdrMassivoDF = getPdrMassivo
  val filterPerimetroAggRic = FilterPerimetroAggRicDummy(pdrMassivoDF)

  def testGetPdrs(): Unit = {
    val pdrsRdd = filterPerimetroAggRic.getPdrs.cache()
    Assert.assertEquals(2, pdrsRdd.count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_1")).count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_2")).count())
    Assert.assertEquals(0, pdrsRdd.filter(_.equals("cod_pdr_3")).count())
    Assert.assertEquals(1, pdrsRdd.filter(_.equals("cod_pdr_4")).count())
  }

  def testFilter(): Unit = {
    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "cod_pdr_1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_2", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_3", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_4", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "cod_pdr_5", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val filteredFlows = filterPerimetroAggRic.filter(`measures`).cache()

    Assert.assertEquals(3, filteredFlows.count())
    Assert.assertEquals(2, filteredFlows.filter(_.pdr == "cod_pdr_1").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "cod_pdr_2").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "cod_pdr_3").count())
    Assert.assertEquals(1, filteredFlows.filter(_.pdr == "cod_pdr_4").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "cod_pdr_5").count())
  }

  def testFilterPdrMassivo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val dummyPdrMassivo: DataFrame = List(
      ("cod_pdr_1", "id1"),
      ("cod_pdr_2", "id26"),
      ("cod_pdr_3", "id13"),
      ("cod_pdr_4", "id64"),
      ("cod_pdr_5", "id50")
    ).toDF(PdrMassivoSchema.codice_pdr, PdrMassivoSchema.n_id_udb)

    val filteredPdrMassivo = filterPerimetroAggRic.filterPdrMassivo(dummyPdrMassivo).cache()

    Assert.assertEquals(2, filteredPdrMassivo.count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_1").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_2").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_3").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_4").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "cod_pdr_5").count())

  }

  case class FilterPerimetroAggRicDummy(pdrMassivo: DataFrame) extends FilterPerimetroAggRic(pdrMassivo) {

    override def getCaPreFinal: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._

      List(
        ("cod_pdr_1", "2021", 1, "Y"),
        ("cod_pdr_1", "2021", 2, "Y"),
        ("cod_pdr_1", "2021", 3, "Y"),
        ("cod_pdr_2", "2021", 4, "M"),
        ("cod_pdr_2", "2021", 5, "M"),
        ("cod_pdr_2", "2020", 6, "Y"),
        ("cod_pdr_3", "2021", 7, "Y"),
        ("cod_pdr_3", "2021", 8, "Y"),
        ("cod_pdr_3", "2020", 9, "Y"),
        ("cod_pdr_4", "2021", 10, "Y"),
        ("cod_pdr_4", "2020", 11, "Y"),
        ("cod_pdr_4", "2020", 12, "Y")
      ).toDF(
        CaPreFinalSchema.codice_pdr,
        CaPreFinalSchema.anno_competenza,
        CaPreFinalSchema.executionid,
        CaPreFinalSchema.trattamento
      )
        .withColumn(CaPreFinalSchema.trattamento_forced, lit(null))
    }

    override def getCa: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._

      List(
        ("cod_pdr_1", 3, 5),
        ("cod_pdr_1", 5, 2),
        ("cod_pdr_2", 4, 3),
        ("cod_pdr_2", 6, 1),
        ("cod_pdr_3", 8, 9),
        ("cod_pdr_3", 9, 6),
        ("cod_pdr_4", 10, 9),
        ("cod_pdr_4", 12, 7)
      ).toDF(
        CaSchema.pdr,
        CaSchema.executionid,
        CaSchema.idCaErrorCode
      )
        .withColumn(CaSchema.executionid, col(CaSchema.executionid).cast(LongType))
        .withColumn(CaSchema.idCaErrorCode, col(CaSchema.idCaErrorCode).cast(IntegerType))
    }

    override def getCodProfStdDaTds: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._

      List(
        ("cod_pdr_1", 2020),
        ("cod_pdr_2", 2020),
        ("cod_pdr_3", 2021),
        ("cod_pdr_4", 2020),
        ("cod_pdr_5", 2021),
        ("cod_pdr_6", 2021),
        ("cod_pdr_7", 2021),
        ("cod_pdr_8", 2021)
      ).toDF(
        CodProfStdDaTdsSchema.codice_pdr,
        CodProfStdDaTdsSchema.anno_competenza
      )
    }
  }

  def getPdrMassivo: DataFrame = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("cod_pdr_1", "Y"),
      ("cod_pdr_1", "M"),
      ("cod_pdr_2", "M"),
      ("cod_pdr_2", "Y"),
      ("cod_pdr_3", "Y"),
      ("cod_pdr_4", "Y"),
      ("cod_pdr_4", "M"),
      ("cod_pdr_5", "Y"),
      ("cod_pdr_5", "M")
    ).toDF(
      PdrMassivoSchema.codice_pdr,
      PdrMassivoSchema.trattamento
    )
  }

}
