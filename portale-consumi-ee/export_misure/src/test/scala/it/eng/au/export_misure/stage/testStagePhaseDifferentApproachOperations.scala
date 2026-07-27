package it.eng.au.export_misure.stage

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure._
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2Schema, misureMensiliCSchema, misureOrarieCSchema}
import it.eng.au.portale_consumi_ee.trasformations.{stagePhaseTrasformation, stagePhaseTrasformationDifferentAprroach}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class testStagePhaseDifferentApproachOperations extends EnvironmentSparkTest{

  val spark = EnvironmentMisure.getSpark

  import spark.implicits._

  def testAutolettureGeneration(): Unit = {
    val autolettureDS = Seq(
      autolettureModel("CF001", "ID001", "POD001", 202401, 1706784000L, 100.5, 30.0, 20.0, 25.5, 10.0, 5.5, 9.5),
      autolettureModel("CF002", "ID002", "POD002", 202402, 1709366400L, 120.0, 40.0, 25.0, 28.0, 12.0, 6.0, 9.0),
      autolettureModel("CF003", "ID003", "POD003", 202403, 1712044800L, 130.0, 45.0, 30.0, 29.0, 13.0, 7.0, 8.5),
      autolettureModel("CF004", "ID004", "POD004", 202404, 1714636800L, 140.5, 50.0, 35.0, 32.5, 14.0, 7.5, 8.0),
      autolettureModel("CF005", "ID005", "POD005", 202405, 1717315200L, 150.5, 55.0, 38.0, 35.5, 15.0, 8.0, 7.5),
      autolettureModel("CF006", "ID006", "POD006", 202406, 1719907200L, 160.0, 60.0, 40.0, 37.0, 16.0, 8.5, 7.0),
      autolettureModel("CF007", "ID007", "POD007", 202407, 1722585600L, 170.0, 65.0, 42.0, 39.5, 17.0, 9.0, 6.5),
      autolettureModel("CF008", "ID008", "POD008", 202408, 1725177600L, 180.0, 70.0, 44.0, 41.0, 18.0, 9.5, 6.0),
      autolettureModel("CF009", "ID009", "POD009", 202409, 1727856000L, 190.0, 75.0, 46.0, 43.5, 19.0, 10.0, 5.5),
      autolettureModel("CF010", "ID010", "POD010", 202410, 1730448000L, 200.0, 80.0, 48.0, 45.0, 20.0, 10.5, 5.0)
    ).toDS()
    val misureMensiliCDS = Seq(misureMensiliCModel()).toDS()
    val misureNonOrarieCDS = Seq(misureNonOrarieCModel()).toDS()
    val misureOrarieCDS = Seq(misureOrarieCModel()).toDS()
    val voltureDS = Seq(voltureModel()).toDS()

    val autoletturaFinalDS = stagePhaseTrasformationDifferentAprroach.autolettureGeneration(autolettureDS)

    autoletturaFinalDS.show()

//    autoletturaDS.select(col("autoletture.lettura_misura_f3")).show()
//    val stageDS = stagePhaseTrasformationDifferentAprroach.calcolo_stage(autolettureDS,misureMensiliCDS,misureNonOrarieCDS,misureOrarieCDS,voltureDS)



//    stageDS.show()
  }

  def testvoltureGeneration(): Unit = {
    val voltureDS = Seq(
      voltureModel("CF001", "ID001", "POD001", "FLUSSO1", 202401, 1706784000L, 100.5, 30.0, 20.0, 25.5, 10.0, 5.5, 9.5),
      voltureModel("CF002", "ID002", "POD002", "FLUSSO2", 202402, 1709366400L, 120.0, 40.0, 25.0, 28.0, 12.0, 6.0, 9.0),
      voltureModel("CF003", "ID003", "POD003", "FLUSSO1", 202403, 1712044800L, 130.0, 45.0, 30.0, 29.0, 13.0, 7.0, 8.5),
      voltureModel("CF004", "ID004", "POD004", "FLUSSO2", 202404, 1714636800L, 140.5, 50.0, 35.0, 32.5, 14.0, 7.5, 8.0),
      voltureModel("CF005", "ID005", "POD005", "FLUSSO1", 202405, 1717315200L, 150.5, 55.0, 38.0, 35.5, 15.0, 8.0, 7.5),
      voltureModel("CF006", "ID006", "POD006", "FLUSSO2", 202406, 1719907200L, 160.0, 60.0, 40.0, 37.0, 16.0, 8.5, 7.0),
      voltureModel("CF007", "ID007", "POD007", "FLUSSO1", 202407, 1722585600L, 170.0, 65.0, 42.0, 39.5, 17.0, 9.0, 6.5),
      voltureModel("CF008", "ID008", "POD008", "FLUSSO2", 202408, 1725177600L, 180.0, 70.0, 44.0, 41.0, 18.0, 9.5, 6.0),
      voltureModel("CF009", "ID009", "POD009", "FLUSSO1", 202409, 1727856000L, 190.0, 75.0, 46.0, 43.5, 19.0, 10.0, 5.5),
      voltureModel("CF010", "ID010", "POD010", "FLUSSO2", 202410, 1730448000L, 200.0, 80.0, 48.0, 45.0, 20.0, 10.5, 5.0)
    ).toDS()


    val voltureFinalDS = stagePhaseTrasformationDifferentAprroach.voltureGeneration(voltureDS)

    voltureFinalDS.show()

//        voltureFinalDS.select(col("volture.tipo_misura")).show()
  }

  def testMisureMensiliGeneration(): Unit = {

    val misureMensiliCDS = Seq(
        misureMensiliCModel("CF001", "ID001", "TIPO1", 1000, 10.5, 300.0, 200.0, 150.0, 100.0, 50.0, 30.0, 5.0, 4.5, 3.5, 2.5, 1.5, 1.0, "POD001", "FLUSSO1", 20250110L, 202401, "Y"),
        misureMensiliCModel("CF002", "ID001", "TIPO2_updated", 1100, 11.0, 320.0, 210.0, 160.0, 110.0, 55.0, 32.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.5, "POD001", "FLUSSO2_updated", 20250115L, 202401, "N"),
        misureMensiliCModel("CF003", "ID001", "TIPO1_last", 1200, 12.5, 340.0, 220.0, 170.0, 120.0, 60.0, 35.0, 7.0, 5.5, 4.5, 3.5, 2.5, 2.0, "POD001", "FLUSSO1_last", 20250120L, 202401, "Y"),
        misureMensiliCModel("CF004", "ID004", "TIPO2_type1", 1300, 13.0, 360.0, 230.0, 180.0, 130.0, 65.0, 38.0, 8.0, 6.0, 5.0, 4.0, 3.0, 2.5, "POD004", "FLUSSO2_type_one", 20250215L, 202402, "N"),
        misureMensiliCModel("CF005", "ID004", "TIPO1_type2", 1400, 14.5, 380.0, 240.0, 190.0, 140.0, 70.0, 40.0, 9.0, 6.5, 5.5, 4.5, 3.5, 3.0, "POD005", "FLUSSO1_type_two", 20250210L, 202402, "Y"),
        misureMensiliCModel("CF006", "ID006", "TIPO2", 1500, 15.0, 400.0, 250.0, 200.0, 150.0, 75.0, 42.0, 10.0, 7.0, 6.0, 5.0, 4.0, 3.5, "POD006", "FLUSSO2", 20250315L, 202406, "N"),
        misureMensiliCModel("CF007", "ID007", "TIPO1", 1600, 16.5, 420.0, 260.0, 210.0, 160.0, 80.0, 45.0, 11.0, 7.5, 6.5, 5.5, 4.5, 4.0, "POD007", "FLUSSO1", 20250415L, 202407, "Y"),
        misureMensiliCModel("CF008", "ID008", "TIPO2", 1700, 17.0, 440.0, 270.0, 220.0, 170.0, 85.0, 48.0, 12.0, 8.0, 7.0, 6.0, 5.0, 4.5, "POD008", "FLUSSO2", 20250515L, 202408, "N"),
        misureMensiliCModel("CF009", "ID009", "TIPO1", 1800, 18.5, 460.0, 280.0, 230.0, 180.0, 90.0, 50.0, 13.0, 8.5, 7.5, 6.5, 5.5, 5.0, "POD009", "FLUSSO1", 20250615L, 202409, "Y"),
        misureMensiliCModel("CF010", "ID010", "TIPO2", 1900, 19.0, 480.0, 290.0, 240.0, 190.0, 95.0, 52.0, 14.0, 9.0, 8.0, 7.0, 6.0, 5.5, "POD010", "FLUSSO2", 20250715L, 202410, "N")
    ).toDS()


    val misureMensiliCFinalDS = stagePhaseTrasformationDifferentAprroach.misureMensiliGeneration(misureMensiliCDS)

    misureMensiliCFinalDS.show()

        misureMensiliCFinalDS.select(col(misureMensiliCSchema.n_id_fornitura),
          col(misureMensiliCSchema.pod),
          col(misureMensiliCSchema.competenza_consumi),
          col("misure_mensili.tipo_misura")).show()
    //    val stageDS = stagePhaseTrasformationDifferentAprroach.calcolo_stage(autolettureDS,misureMensiliCDS,misureNonOrarieCDS,misureOrarieCDS,voltureDS)



    //    stageDS.show()
  }


  def testMisureNonOrarieGeneration(): Unit = {

    val misureNonOrarieCDS = Seq(
      misureNonOrarieCModel("CF001", "ID001", 202401, "POD001", "TIPO1", 1000.5, 300.0, 200.0, 150.0, 100.0, 50.0, 30.0, 5.0, 4.5, 3.5, 2.5, 1.5, 1.0, 10.5, "FLUSSO1", 1706784000L, 5.0, 4.5, 3.5, 6.0, "Y"),
      misureNonOrarieCModel("CF002", "ID002", 202402, "POD002", "TIPO2", 1100.0, 320.0, 210.0, 160.0, 110.0, 55.0, 32.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.5, 11.0, "FLUSSO2", 1709366400L, 5.5, 5.0, 4.0, 6.5, "N"),
      misureNonOrarieCModel("CF003", "ID003", 202403, "POD003", "TIPO1", 1200.5, 340.0, 220.0, 170.0, 120.0, 60.0, 35.0, 7.0, 5.5, 4.5, 3.5, 2.5, 2.0, 12.5, "FLUSSO1", 1712044800L, 6.0, 5.5, 4.5, 7.0, "Y"),
      misureNonOrarieCModel("CF004", "ID004", 202404, "POD004", "TIPO2", 1300.0, 360.0, 230.0, 180.0, 130.0, 65.0, 38.0, 8.0, 6.0, 5.0, 4.0, 3.0, 2.5, 13.0, "FLUSSO2", 1714636800L, 6.5, 6.0, 5.0, 7.5, "N"),
      misureNonOrarieCModel("CF005", "ID005", 202405, "POD005", "TIPO1", 1400.5, 380.0, 240.0, 190.0, 140.0, 70.0, 40.0, 9.0, 6.5, 5.5, 4.5, 3.5, 3.0, 14.5, "FLUSSO1", 1717315200L, 7.0, 6.5, 5.5, 8.0, "Y"),
      misureNonOrarieCModel("CF006", "ID006", 202406, "POD006", "TIPO2", 1500.0, 400.0, 250.0, 200.0, 150.0, 75.0, 42.0, 10.0, 7.0, 6.0, 5.0, 4.0, 3.5, 15.0, "FLUSSO2", 1719907200L, 7.5, 7.0, 6.0, 8.5, "N"),
      misureNonOrarieCModel("CF007", "ID007", 202407, "POD007", "TIPO1", 1600.5, 420.0, 260.0, 210.0, 160.0, 80.0, 45.0, 11.0, 7.5, 6.5, 5.5, 4.5, 4.0, 16.5, "FLUSSO1", 1722585600L, 8.0, 7.5, 6.5, 9.0, "Y"),
      misureNonOrarieCModel("CF008", "ID008", 202408, "POD008", "TIPO2", 1700.0, 440.0, 270.0, 220.0, 170.0, 85.0, 48.0, 12.0, 8.0, 7.0, 6.0, 5.0, 4.5, 17.0, "FLUSSO2", 1725177600L, 8.5, 8.0, 7.0, 9.5, "N"),
      misureNonOrarieCModel("CF009", "ID009", 202409, "POD009", "TIPO1", 1800.5, 460.0, 280.0, 230.0, 180.0, 90.0, 50.0, 13.0, 8.5, 7.5, 6.5, 5.5, 5.0, 18.5, "FLUSSO1", 1727856000L, 9.0, 8.5, 7.5, 10.0, "Y"),
      misureNonOrarieCModel("CF010", "ID010", 202410, "POD010", "TIPO2", 1900.0, 480.0, 290.0, 240.0, 190.0, 95.0, 52.0, 14.0, 9.0, 8.0, 7.0, 6.0, 5.5, 19.0, "FLUSSO2", 1730448000L, 9.5, 9.0, 8.0, 10.5, "N")
    ).toDS()

    val misureNonOrarieCFinalDS = stagePhaseTrasformationDifferentAprroach.misureNonOrarieGeneration(misureNonOrarieCDS)

    misureNonOrarieCFinalDS.show()

  }

  def testMisureOrarieGeneration(): Unit = {

    val misureOrarieCDS = Seq(
      misureOrarieCModel("CF001", "ID001", "POD001", 14, "TIPO1", 50.5, 100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 5.0, 4.5, 3.5, 2.5, 1.5, 1.0, 500.0, "Y", "FLUSSO1", 20240114L, 202401),
      misureOrarieCModel("CF002", "ID001", "POD001", 28, "TIPO2", 52.0, 110.0, 95.0, 85.0, 75.0, 65.0, 55.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.5, 520.0, "N", "FLUSSO2", 20240128L, 202401),
      misureOrarieCModel("CF003", "ID001", "POD001", 1, "TIPO1", 53.5, 120.0, 100.0, 90.0, 80.0, 70.0, 60.0, 7.0, 5.5, 4.5, 3.5, 2.5, 2.0, 540.0, "Y", "FLUSSO1", 20240101L, 202401),
      misureOrarieCModel("CF004", "ID001", "POD001", 16, "TIPO2", 55.0, 130.0, 105.0, 95.0, 85.0, 75.0, 65.0, 8.0, 6.0, 5.0, 4.0, 3.0, 2.5, 560.0, "N", "FLUSSO2", 20240116L, 202401),
      misureOrarieCModel("CF005", "ID001", "POD001", 3, "TIPO1", 56.5, 140.0, 110.0, 100.0, 90.0, 80.0, 70.0, 9.0, 6.5, 5.5, 4.5, 3.5, 3.0, 580.0, "Y", "FLUSSO1", 20240103L, 202401),
      misureOrarieCModel("CF006", "ID006", "ID006", 23, "TIPO2", 58.0, 150.0, 115.0, 105.0, 95.0, 85.0, 75.0, 10.0, 7.0, 6.0, 5.0, 4.0, 3.5, 600.0, "N", "FLUSSO2", 20240423L, 202404),
      misureOrarieCModel("CF007", "ID006", "ID006", 17, "TIPO1", 59.5, 160.0, 120.0, 110.0, 100.0, 90.0, 80.0, 11.0, 7.5, 6.5, 5.5, 4.5, 4.0, 620.0, "Y", "FLUSSO1", 20240417L, 202404),
      misureOrarieCModel("CF008", "ID006", "ID006", 10, "TIPO2", 61.0, 170.0, 125.0, 115.0, 105.0, 95.0, 85.0, 12.0, 8.0, 7.0, 6.0, 5.0, 4.5, 640.0, "N", "FLUSSO2", 20240410L, 202404),
      misureOrarieCModel("CF009", "ID006", "ID006", 9, "TIPO1", 62.5, 180.0, 130.0, 120.0, 110.0, 100.0, 90.0, 13.0, 8.5, 7.5, 6.5, 5.5, 5.0, 660.0, "Y", "FLUSSO1", 20240409L, 202404),
      misureOrarieCModel("CF010", "ID010", "POD010", 1, "TIPO2", 64.0, 190.0, 135.0, 125.0, 115.0, 105.0, 95.0, 14.0, 9.0, 8.0, 7.0, 6.0, 5.5, 680.0, "N", "FLUSSO2", 20240401L, 202401)
    ).toDS()

    val misureOrarieCFinalDS = stagePhaseTrasformationDifferentAprroach.misureOrarieGeneration(misureOrarieCDS)

    misureOrarieCFinalDS.show()

    misureOrarieCFinalDS.select(col(misureOrarieCSchema.n_id_fornitura),
      col(misureOrarieCSchema.pod),
      col(misureOrarieCSchema.competenza_consumi),
      col("misure_orarie")).show(false)
    //    stageDS.show()
  }

  def testCalcoloStage(): Unit = {
    val autolettureDS = Seq(
      autoletturaRevisitedModel("ID001", "POD001", 202401, AutoletturaValues("202401", "1706784000", "100.5", "200.0", "150.0", "125.0", "100.0", "75.0", "50.0"), "hashed1"),
        autoletturaRevisitedModel("ID001", "POD001", 202402, AutoletturaValues("202402", "1706870400", "110.5", "210.0", "160.0", "130.0", "105.0", "80.0", "55.0"), "hashed2"),
        autoletturaRevisitedModel("ID001", "POD001", 202403, AutoletturaValues("202403", "1706956800", "120.5", "220.0", "170.0", "135.0", "110.0", "85.0", "60.0"), "hashed3"),
        autoletturaRevisitedModel("ID001", "POD001", 202404, AutoletturaValues("202404", "1707043200", "130.5", "230.0", "180.0", "140.0", "115.0", "90.0", "65.0"), "hashed4"),
        autoletturaRevisitedModel("ID001", "POD001", 202405, AutoletturaValues("202405", "1707129600", "140.5", "240.0", "190.0", "145.0", "120.0", "95.0", "70.0"), "hashed5"),
        autoletturaRevisitedModel("ID001", "POD001", 202406, AutoletturaValues("202406", "1707216000", "150.5", "250.0", "200.0", "150.0", "125.0", "100.0", "75.0"), "hashed6")
    ).toDS()
    val misureMensiliCDS = Seq(
      misureMensiliCRevisitedModel("ID001", "POD001", 202401, misureMensiliCStructValues("202401", "100.5", "100", "200.0", "150.0", "125.0", "100.0", "75.0", "50.0", "20.0", "15.0", "10.0", "5.0", "0.0", "0.0", "FLUSSO1", "1706784000"), "hashed1"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202402, misureMensiliCStructValues("202402", "110.5", "110", "210.0", "160.0", "130.0", "105.0", "80.0", "55.0", "21.0", "16.0", "11.0", "6.0", "1.0", "0.0", "FLUSSO2", "1706870400"), "hashed2"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202403, misureMensiliCStructValues("202403", "120.5", "120", "220.0", "170.0", "135.0", "110.0", "85.0", "60.0", "22.0", "17.0", "12.0", "7.0", "2.0", "0.0", "FLUSSO3", "1706956800"), "hashed3"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202404, misureMensiliCStructValues("202404", "130.5", "130", "230.0", "180.0", "140.0", "115.0", "90.0", "65.0", "23.0", "18.0", "13.0", "8.0", "3.0", "0.0", "FLUSSO4", "1707043200"), "hashed4"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202405, misureMensiliCStructValues("202405", "140.5", "140", "240.0", "190.0", "145.0", "120.0", "95.0", "70.0", "24.0", "19.0", "14.0", "9.0", "4.0", "0.0", "FLUSSO5", "1707129600"), "hashed5"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202406, misureMensiliCStructValues("202406", "150.5", "150", "250.0", "200.0", "150.0", "125.0", "100.0", "75.0", "25.0", "20.0", "15.0", "10.0", "5.0", "0.0", "FLUSSO6", "1707216000"), "hashed6"),
      misureMensiliCRevisitedModel("ID001", "POD001", 202407, misureMensiliCStructValues("202406", "150.5", "150", "250.0", "200.0", "150.0", "125.0", "100.0", "75.0", "25.0", "20.0", "15.0", "10.0", "5.0", "0.0", "FLUSSO6", "1707216000"), "hashed7")
    ).toDS()


    val misureNonOrarieCDS = Seq(
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202401, misureNonOrarieCStructValues("202401", "100.5", "300.0", "200.0", "150.0", "125.0", "100.0", "75.0", "50.0", "20.0", "15.0", "10.0", "5.0", "0.0", "0.0", "FLUSSO1", "1706784000", "10.0", "20.0", "30.0", "40.0"), "hashed1"),
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202402, misureNonOrarieCStructValues("202402", "110.5", "300.0", "210.0", "160.0", "130.0", "105.0", "80.0", "55.0", "21.0", "16.0", "11.0", "6.0", "1.0", "0.0", "FLUSSO2", "1706870400", "11.0", "21.0", "31.0", "41.0"), "hashed2"),
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202403, misureNonOrarieCStructValues("202403", "120.5", "300.0", "220.0", "170.0", "135.0", "110.0", "85.0", "60.0", "22.0", "17.0", "12.0", "7.0", "2.0", "0.0", "FLUSSO3", "1706956800", "12.0", "22.0", "32.0", "42.0"), "hashed3"),
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202404, misureNonOrarieCStructValues("202404", "130.5", "300.0", "230.0", "180.0", "140.0", "115.0", "90.0", "65.0", "23.0", "18.0", "13.0", "8.0", "3.0", "0.0", "FLUSSO4", "1707043200", "13.0", "23.0", "33.0", "43.0"), "hashed4"),
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202405, misureNonOrarieCStructValues("202405", "140.5", "300.0", "240.0", "190.0", "145.0", "120.0", "95.0", "70.0", "24.0", "19.0", "14.0", "9.0", "4.0", "0.0", "FLUSSO5", "1707129600", "14.0", "24.0", "34.0", "44.0"), "hashed5"),
      misureNonOrarieCRevisitedModel("ID001", "POD001", 202406, misureNonOrarieCStructValues("202406", "150.5", "300.0", "250.0", "200.0", "155.0", "130.0", "105.0", "80.0", "25.0", "20.0", "15.0", "10.0", "5.0", "0.0", "FLUSSO6", "1707216000", "15.0", "25.0", "35.0", "45.0"), "hashed6")
    ).toDS()


    val misureOrarieCDS = Seq(
      misureOrarieCRevisitedModel("ID001", "POD001", 202401,
        List(
          misureOrarieCStructValues("1", "202401", "300.0", "100.5", "200.0", "150.0", "125.0", "100.0", "75.0", "50.0", "20.0", "15.0", "10.0", "5.0", "0.0", "10.0", "FLUSSO1", "1706784000"),
          misureOrarieCStructValues("2", "202401", "300.0", "110.5", "210.0", "160.0", "130.0", "105.0", "80.0", "55.0", "21.0", "16.0", "11.0", "6.0", "1.0", "11.0", "FLUSSO1", "1706870400")
        ),
        "hashed1"
      ),
      misureOrarieCRevisitedModel("ID001", "POD001", 202402,
        List(
          misureOrarieCStructValues("1", "202402", "300.0", "120.5", "220.0", "170.0", "140.0", "115.0", "90.0", "65.0", "22.0", "17.0", "12.0", "7.0", "2.0", "12.0", "FLUSSO2", "1706956800"),
          misureOrarieCStructValues("2", "202402", "300.0", "130.5", "230.0", "180.0", "150.0", "125.0", "100.0", "75.0", "23.0", "18.0", "13.0", "8.0", "3.0", "13.0", "FLUSSO2", "1707043200")
        ),
        "hashed2"
      ),
      misureOrarieCRevisitedModel("ID001", "POD001", 202403,
        List(
          misureOrarieCStructValues("1", "202403", "300.0", "140.5", "240.0", "190.0", "160.0", "135.0", "110.0", "85.0", "24.0", "19.0", "14.0", "9.0", "4.0", "14.0", "FLUSSO3", "1707129600"),
          misureOrarieCStructValues("2", "202403", "300.0", "150.5", "250.0", "200.0", "170.0", "145.0", "120.0", "95.0", "25.0", "20.0", "15.0", "10.0", "5.0", "15.0", "FLUSSO3", "1707216000")
        ),
        "hashed3"
      ),
      misureOrarieCRevisitedModel("ID001", "POD001", 202404,
        List(
          misureOrarieCStructValues("1", "202404", "300.0", "160.5", "260.0", "210.0", "180.0", "155.0", "130.0", "105.0", "26.0", "21.0", "16.0", "11.0", "6.0", "16.0", "FLUSSO4", "1707302400"),
          misureOrarieCStructValues("2", "202404", "300.0", "170.5", "270.0", "220.0", "190.0", "165.0", "140.0", "115.0", "27.0", "22.0", "17.0", "12.0", "7.0", "17.0", "FLUSSO4", "1707388800")
        ),
        "hashed4"
      ),
      misureOrarieCRevisitedModel("ID001", "POD001", 202405,
        List(
          misureOrarieCStructValues("1", "202405", "300.0", "180.5", "280.0", "230.0", "200.0", "175.0", "150.0", "125.0", "28.0", "23.0", "18.0", "13.0", "8.0", "18.0", "FLUSSO5", "1707475200"),
          misureOrarieCStructValues("2", "202405", "300.0", "190.5", "290.0", "240.0", "210.0", "185.0", "160.0", "135.0", "29.0", "24.0", "19.0", "14.0", "9.0", "19.0", "FLUSSO5", "1707561600")
        ),
        "hashed5"
      ),
      misureOrarieCRevisitedModel("ID001", "POD001", 202406,
        List(
          misureOrarieCStructValues("1", "202406", "300.0", "200.5", "300.0", "250.0", "220.0", "195.0", "170.0", "145.0", "30.0", "25.0", "20.0", "15.0", "10.0", "20.0", "FLUSSO6", "1707648000"),
          misureOrarieCStructValues("2", "202406", "300.0", "210.5", "310.0", "260.0", "230.0", "205.0", "180.0", "155.0", "31.0", "26.0", "21.0", "16.0", "11.0", "21.0", "FLUSSO6", "1707734400")
        ),
        "hashed6"
      )
    ).toDS()

    val voltureDS = Seq(
      voltureRevisitedModel("ID001", "POD001", 202401,
        VoltureValues("202401", "1706784000", "100.0", "110.0", "120.0", "130.0", "140.0", "150.0", "FLUSSO1"), "hashed1"
      ),

      voltureRevisitedModel("ID001", "POD001", 202402,
        VoltureValues("202402", "1706870400", "105.0", "115.0", "125.0", "135.0", "145.0", "155.0", "FLUSSO2"), "hashed2"
      ),

      voltureRevisitedModel("ID001", "POD001", 202403,
        VoltureValues("202403", "1706956800", "110.0", "120.0", "130.0", "140.0", "150.0", "160.0", "FLUSSO3"), "hashed3"
      ),

      voltureRevisitedModel("ID001", "POD001", 202404,
        VoltureValues("202404", "1707043200", "115.0", "125.0", "135.0", "145.0", "155.0", "165.0", "FLUSSO4"), "hashed4"
      ),

      voltureRevisitedModel("ID001", "POD001", 202405,
        VoltureValues("202405", "1707129600", "120.0", "130.0", "140.0", "150.0", "160.0", "170.0", "FLUSSO5"), "hashed5"
      ),

      voltureRevisitedModel("ID001", "POD001", 202406,
        VoltureValues("202406", "1707216000", "125.0", "135.0", "145.0", "155.0", "165.0", "175.0", "FLUSSO6"), "hashed6"
      )
    ).toDS()


    val stagelDS = stagePhaseTrasformationDifferentAprroach.calcolo_stage(
                            autolettureDS,
                            misureMensiliCDS,
                            misureNonOrarieCDS,
                            misureOrarieCDS,
                            voltureDS
                    )

    stagelDS.show()

  }

  def testData_compare (): Unit = {

    val currentDate = LocalDate.now()
    val currenyAnnoMese = currentDate.format(DateTimeFormatter.ofPattern("yyyyMM")).toInt

    val etlStageNewDS: Dataset[etlStage3M2ProposedModel] = Seq(
      etlStage3M2ProposedModel("ID001",
        List(misureOrarieCStructValues(giorno = "20240404","202401", "100.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0",data_lettura = "202401")),
        misureMensiliCStructValues("202401", "5.0", "100", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        misureNonOrarieCStructValues("202401", "5.0", "95.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        VoltureValues("202401", "1706784000", "90.0", "95.0", "85.0", "75.0", "65.0", "55.0", "FLUSSO1"),
        AutoletturaValues("202401", "1706784000", "88.0", "78.0", "68.0", "58.0", "48.0", "38.0"),
        "POD001", "COD001", "hashed1_updated", 202401L, 202401
      ),

      etlStage3M2ProposedModel(
        "ID002",
        List(misureOrarieCStructValues(giorno = "20240404", "202402", "110.0", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0",data_lettura = "20240202")),
        misureMensiliCStructValues("202402", "6.0", "110", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0"),
        misureNonOrarieCStructValues("202402", "6.0", "105.0", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0"),
        VoltureValues("202402", "1706870400", "95.0", "100.0", "90.0", "80.0", "70.0", "60.0", "FLUSSO2"),
        AutoletturaValues("202402", "1706870400", "92.0", "82.0", "72.0", "62.0", "52.0", "42.0"),
        "POD002", "COD002", "hashed2_same", 202402, 202402
      ),

      etlStage3M2ProposedModel(
        "ID003",
        List(misureOrarieCStructValues(giorno="20240303", "202403", "120.0", "20.0", "30.0", "40.0","50.0", "60.0", "70.0",data_lettura="20240303")),
        misureMensiliCStructValues("202403", "7.0", "120", "20.0", "30.0", "40.0", "50.0", "60.0", "70.0"),
        misureNonOrarieCStructValues("202403", "7.0", "115.0", "20.0", "30.0", "40.0", "50.0", "60.0", "70.0"),
        VoltureValues("202403", "1706956800", "100.0", "105.0", "95.0", "85.0", "75.0", "65.0", "FLUSSO3"),
        AutoletturaValues("202403", "1706956800", "96.0", "86.0", "76.0", "66.0", "56.0", "46.0"),
        "POD003", "COD003", "hashed3_updated", 202403, 202403
      ),

      etlStage3M2ProposedModel(
        "ID004",
        List(misureOrarieCStructValues(giorno = "20240404", "202404", "130.0", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0",data_lettura = "20240404")),
        misureMensiliCStructValues("202404", "8.0", "130", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0"),
        misureNonOrarieCStructValues("202404", "8.0", "125.0", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0"),
        VoltureValues("202404", "1707043200", "105.0", "110.0", "100.0", "90.0", "80.0", "70.0", "FLUSSO4"),
        AutoletturaValues("202404", "1707043200", "100.0", "90.0", "80.0", "70.0", "60.0", "50.0"),
        "POD004", "COD004", "hashed4_updated", 202404, 202404
      ),
      etlStage3M2ProposedModel(
        "ID005",
        List(misureOrarieCStructValues( giorno = "20240404","202405", "140.0", "30.0", "40.0", "50.0", "60.0", "70.0", "80.0",data_lettura = "20240505" )),
        misureMensiliCStructValues("202405", "9.0", "140", "30.0", "40.0", "50.0", "60.0", "70.0", "80.0"),
        misureNonOrarieCStructValues("202405", "9.0", "135.0", "30.0", "40.0", "50.0", "60.0", "70.0", "80.0"),
        VoltureValues("202405", "1707129600", "110.0", "115.0", "105.0", "95.0", "85.0", "75.0", "FLUSSO5"),
        AutoletturaValues("202405", "1707129600", "104.0", "94.0", "84.0", "74.0", "64.0", "54.0"),
        "POD005", "COD005", "hashed5_new", currenyAnnoMese, currenyAnnoMese
      )

    ).toDS()

    val etlStageOldDS: Dataset[etlStage3M2ProposedModel] = Seq(
      etlStage3M2ProposedModel(
        "ID001",
        List(misureOrarieCStructValues(giorno = "20240404", "202401", "100.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0",data_lettura = "20240101")),
        misureMensiliCStructValues("202401", "5.0", "100", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        misureNonOrarieCStructValues("202401", "5.0", "95.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        VoltureValues("202401", "1706784000", "90.0", "95.0", "85.0", "75.0", "65.0", "55.0", "FLUSSO1"),
        AutoletturaValues("202401", "1706784000", "88.0", "78.0", "68.0", "58.0", "48.0", "38.0"),
        "POD001", "COD001", "hashed1_old", 202401, 202401
      )
,

      etlStage3M2ProposedModel(
        "ID002",
        List(misureOrarieCStructValues( giorno = "20240404","202402", "110.0", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0",data_lettura ="20240202")),
        misureMensiliCStructValues("202402", "6.0", "110", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0"),
        misureNonOrarieCStructValues("202402", "6.0", "105.0", "15.0", "25.0", "35.0", "45.0", "55.0", "65.0"),
        VoltureValues("202402", "1706870400", "95.0", "100.0", "90.0", "80.0", "70.0", "60.0", "FLUSSO2"),
        AutoletturaValues("202402", "1706870400", "92.0", "82.0", "72.0", "62.0", "52.0", "42.0"),
        "POD002", "COD002", "hashed2_same", 202402, 202402
      ),
      etlStage3M2ProposedModel(
        "ID003",
        List(misureOrarieCStructValues(giorno = "20240404", "202403", "120.0", "20.0", "30.0", "40.0", "50.0", "60.0", "70.0",data_lettura = "20240303")),
        misureMensiliCStructValues("202403", "7.0", "120", "20.0", "30.0", "40.0", "50.0", "60.0", "70.0"),
        misureNonOrarieCStructValues("202403", "7.0", "115.0", "20.0", "30.0", "40.0", "50.0", "60.0", "70.0"),
        VoltureValues("202403", "1706956800", "100.0", "105.0", "95.0", "85.0", "75.0", "65.0", "FLUSSO3"),
        AutoletturaValues("202403", "1706956800", "96.0", "86.0", "76.0", "66.0", "56.0", "46.0"),
        "POD003", "COD003", "hashed3", 202403, 202403
      ),
      etlStage3M2ProposedModel(
        "ID004",
        List(misureOrarieCStructValues(giorno = "20240404", "202404", "130.0", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0",data_lettura = "20240404")),
        misureMensiliCStructValues("202404", "8.0", "130", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0"),
        misureNonOrarieCStructValues("202404", "8.0", "125.0", "25.0", "35.0", "45.0", "55.0", "65.0", "75.0"),
        VoltureValues("202404", "1707043200", "105.0", "110.0", "100.0", "90.0", "80.0", "70.0", "FLUSSO4"),
        AutoletturaValues("202404", "1707043200", "100.0", "90.0", "80.0", "70.0", "60.0", "50.0"),
        "POD004", "COD004", "hashed4", 202404, 202404
      )
    ).toDS()

    val (newDS,updatetDs,writeOnHive) = stagePhaseTrasformationDifferentAprroach.data_compare(etlStageNewDS,etlStageOldDS)

    newDS.show()
    updatetDs.show()

//    def applyTableFilter(df : Dataset[etlStage3M2Model],tableValue: Int): Long = {
//      df.filter(col(etlStage3M2Schema.tabella) === tableValue).count()
//    }
//
//    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,1),applyTableFilter(result,1) )
//    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,2),applyTableFilter(result,2) )
//    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,3),applyTableFilter(result,3) )
//    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,4),applyTableFilter(result,4) )
//    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,5),applyTableFilter(result,5) )
  }


  def testControlloEtlPrecedente (): Unit = {
    val etlStage3M2OKDS = Seq(
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 202501, 1, "POD0000000002", "02", 1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 202501, 2, "POD0000000002", "02", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3old", "", "", "", "", 202501, 3, "POD0000000003", "03", 2, "hash4old"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 202501, 4, "POD0000000003", "03", 2, "hash5"),
      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD0000000004", "04", 3, "hash7"),
      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD0000000004", "04", 3, "hash8"),
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD0000000005", "05", 4, "hash2"),
      etlStage3M2Model("POD003", "prova2", "", "", "", "", 202502, 2, "POD0000000005", "05", 4, "hash3"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD0000000006", "06", 5, "hash4"),
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD0000000006", "06", 5, "hash2"),
      etlStage3M2Model("POD004", "prova2", "", "", "", "", 202410, 2, "POD0000000007", "07", 1, "hash3"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 3, "POD0000000007", "07", 1, "hash4"),
      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD0000000008", "08", 2, "hash2"),
      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD0000000008", "08", 2, "hash3"),
      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD0000000009", "09", 3, "hash4"),
      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD0000000010", "10", 3, "hash5"),
      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD0000000011", "11", 4, "hash6"),
      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD0000000012", "12", 4, "hash7"),
      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD0000000012", "12", 5, "hash8"),
      etlStage3M2Model("POD005", "prova8", "", "", "", "", 202411, 8, "POD0000000013", "13", 5, "hash9")
    ).toDS()

    val etlStage3M2NotOkCountDS = Seq(
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 20250101, 1, "POD0000000002", "02", 1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 20250102, 2, "POD0000000002", "02", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3old", "", "", "", "", 20250103, 3, "POD0000000003", "03", 2, "hash4old"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 20250104, 4, "POD0000000003", "03", 2, "hash5"),
      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD0000000004", "04", 3, "hash7"),
      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD0000000004", "04", 3, "hash8"),
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD0000000005", "05", 4, "hash2"),
      etlStage3M2Model("POD003", "prova2", "", "", "", "", 202502, 2, "POD0000000005", "05", 4, "hash3"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD0000000006", "06", 5, "hash4"),
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD0000000006", "06", 5, "hash2"),
      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD0000000008", "08", 2, "hash2"),
      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD0000000008", "08", 2, "hash3"),
      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD0000000009", "09", 3, "hash4"),
      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD0000000010", "10", 3, "hash5"),
      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD0000000011", "11", 4, "hash6"),
      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD0000000012", "12", 4, "hash7"),
      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD0000000012", "12", 5, "hash8"),
      etlStage3M2Model("POD005", "prova8", "", "", "", "", 202411, 8, "POD0000000013", "13", 5, "hash9")
    ).toDS()

    val etlStage3M2NotOkLastCodPodDS = Seq(
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 20250101, 1, "POD0000000002", "02", 1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 20250102, 2, "POD0000000002", "02", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3old", "", "", "", "", 20250103, 3, "POD0000000003", "03", 2, "hash4old"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 20250104, 4, "POD0000000003", "03", 2, "hash5"),
      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD0000000004", "04", 3, "hash7"),
      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD0000000004", "04", 3, "hash8"),
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD0000000005", "05", 4, "hash2"),
      etlStage3M2Model("POD003", "prova2", "", "", "", "", 202502, 2, "POD0000000005", "05", 4, "hash3"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD0000000006", "06", 5, "hash4"),
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD0000000006", "06", 5, "hash2"),
      etlStage3M2Model("POD004", "prova2", "", "", "", "", 202410, 2, "POD0000000007", "07", 1, "hash3"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 3, "POD0000000007", "07", 1, "hash4"),
      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD0000000008", "08", 2, "hash2"),
      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD0000000008", "08", 2, "hash3"),
      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD0000000009", "09", 3, "hash4"),
      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD0000000010", "10", 3, "hash5"),
      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD0000000011", "11", 4, "hash6"),
      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD0000000012", "12", 4, "hash7"),
      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD0000000012", "12", 5, "hash8")
    ).toDS()

    val registroLoadDS: Dataset[registroLoadModel] = Seq(
      registroLoadModel("Note A", 10L,20250101L,202411,"RUN001" ),
      registroLoadModel( "Note B", 20L,20250101L,202411,"RUN002"),
      registroLoadModel( "Note C", 30L,20250101L,202410,"RUN003"),
      registroLoadModel( "Note D", 40L,20250101L,202410,"RUN004"),
      registroLoadModel("Note E", 50L,20250101L,202412,"RUN005"),
      registroLoadModel( "Note F", 60L,20250101L,202412,"RUN006"),
      registroLoadModel( "Note G", 70L,20250101L,202501,"RUN007"),
      registroLoadModel( "Note H", 80L,20250101L,202501,"RUN008"),
      registroLoadModel( "Note I", 90L,20250101L,202501,"RUN009"),
      registroLoadModel( "Note J", 100L,20250101L,202502,"RUN010"),
      registroLoadModel( "Note K", 110L,20250101L,202502,"RUN011"),
      registroLoadModel( "Note L", 120L,20250101L,202502,"RUN012")
    ).toDS()

    Assert.assertTrue(stagePhaseTrasformation.controlloEtlPrecedente(etlStage3M2OKDS,registroLoadDS))
    Assert.assertFalse(stagePhaseTrasformation.controlloEtlPrecedente(etlStage3M2NotOkCountDS,registroLoadDS))
    Assert.assertFalse(stagePhaseTrasformation.controlloEtlPrecedente(etlStage3M2NotOkLastCodPodDS,registroLoadDS))
  }

}
