package it.eng.au.export_misure.stage

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{autolettureModel, etlStage3M2Model, misureMensiliCModel, misureNonOrarieCModel, misureOrarieCModel, registroLoadModel, voltureModel}
import it.eng.au.portale_consumi_ee.schema.misure.etlStage3M2Schema
import it.eng.au.portale_consumi_ee.trasformations.stagePhaseTrasformation
import junit.framework.Test
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.{Assert, Ignore}

class testStagePhaseOperations extends EnvironmentSparkTest{

  val spark = EnvironmentMisure.getSpark

  import spark.implicits._

  def testCalcoloStage(): Unit = {
    val autolettureDS = Seq(autolettureModel()).toDS()
    val misureMensiliCDS = Seq(misureMensiliCModel()).toDS()
    val misureNonOrarieCDS = Seq(misureNonOrarieCModel()).toDS()
    val misureOrarieCDS = Seq(misureOrarieCModel()).toDS()
    val voltureDS = Seq(voltureModel()).toDS()

    val stageDS = stagePhaseTrasformation.calcolo_stage(autolettureDS,misureMensiliCDS,misureNonOrarieCDS,misureOrarieCDS,voltureDS)

    stageDS.show()
  }

  def testStageHash (): Unit = {

    val etlStage3M2NowMisureOraraGGDS = Seq(
//      etlStage3M2Model(fornitura_pod = "POD001", misure_orarie = "2023-01-01", misure_mensili = "",misure_non_orarie= "", volture = "",autoletture =  "",competenza_consumi = 100,giorno = 1,pod = "POD001", tabella = 1,hash_value = "hash1"),
// for fornitura_pod POD002 , competenze consimi 202501 , day 3 update data and hash value consequently, day 5 new data not present in previous footprint
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 202501, 1, "POD002", "XX",1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 202501, 2, "POD002", "XX", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3new", "", "", "", "", 202501, 3, "POD002", "XX", 1, "hash4new"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 202501, 4, "POD002", "XX", 1, "hash5"),
      etlStage3M2Model("POD002", "prova5", "", "", "", "", 202501, 5, "POD002", "XX", 1, "hash6"),
// for fornitura_pod POD002 , competenze consiumi 202412 nothing appen respect to previous footprint
      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD002", "XX", 1, "hash7"),
      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD002", "XX", 1, "hash8"),
// for fornitura_pod POD003 , competenze consimi 202501 , day 2 update data and hash value consequently
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD003", "XX", 1, "hash2"),
      etlStage3M2Model("POD003", "prova2new", "", "", "", "", 202502, 2, "POD003", "XX", 1, "hash3new"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD003", "XX", 1, "hash4"),
// for fornitura_pod POD004 , competenze consimi 202410 , day 4 new data
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD004", "XX", 1, "hash2"),
      etlStage3M2Model("POD004", "prova2", "", "", "", "", 202410, 2, "POD004", "XX", 1, "hash3"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 3, "POD004", "XX", 1, "hash4"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 4, "POD004", "XX", 1, "hash4"),
 // for fornitura_pod POD005 , competenze consimi 202411 nothing happen
      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD004", "XX", 1, "hash2"),
      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD004", "XX", 1, "hash3"),
      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD004", "XX", 1, "hash4"),
      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD004", "XX", 1, "hash5"),
      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD004", "XX", 1, "hash6"),
      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD004", "XX", 1, "hash7"),
      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD004", "XX", 1, "hash8"),
      etlStage3M2Model("POD005", "prova8", "", "", "", "", 202411, 8, "POD004", "XX", 1, "hash9")
    ).toDS()
    //    stageFinalNowDS: Dataset[etlStage3M2Model]

    val etlStage3M2CompareMisureOraraGDS = Seq(
//      etlStage3M2Model(fornitura_pod = "POD001", misure_orarie = "2023-01-01", misure_mensili = "2023-01",misure_non_orarie= "120", volture = "1",autoletture =  "10",competenza_consumi = 100,giorno = 1,pod = "POD001",cod_pod =  "XX", tabella = 1,hash_value = "hash1"),
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 20250101, 1, "POD002", "XX", 1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 20250102, 2, "POD003", "XX", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3old", "", "", "", "", 20250103, 3, "POD004", "XX", 1, "hash4old"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 20250104, 4, "POD005", "XX", 1, "hash5"),
      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD002", "XX", 1, "hash7"),
      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD002", "XX", 1, "hash8"),
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD003", "XX", 1, "hash2"),
      etlStage3M2Model("POD003", "prova2", "", "", "", "", 202502, 2, "POD003", "XX", 1, "hash3"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD003", "XX", 1, "hash4"),
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD004", "XX", 1, "hash2"),
      etlStage3M2Model("POD004", "prova2", "", "", "", "", 202410, 2, "POD004", "XX", 1, "hash3"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 3, "POD004", "XX", 1, "hash4"),
      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD004", "XX", 1, "hash2"),
      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD004", "XX", 1, "hash3"),
      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD004", "XX", 1, "hash4"),
      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD004", "XX", 1, "hash5"),
      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD004", "XX", 1, "hash6"),
      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD004", "XX", 1, "hash7"),
      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD004", "XX", 1, "hash8"),
      etlStage3M2Model("POD005", "prova8", "", "", "", "", 202411, 8, "POD004", "XX", 1, "hash9")
    ).toDS()

    val etlStage3M2OtherMisureGDS = Seq(
      // for fornitura_pod POD002, competenze consimi 202501
      // table 2 update data and hash value consequently,
      // table 3  new data not present in previous footprint
      //table 4 nothing happen
      etlStage3M2Model("POD002", "", "prova1update", "", "", "", 202501, 1, "POD002", "XX", 2, "hash2update"),
      etlStage3M2Model("POD002", "", "", "prova2new", "", "", 202501, 1, "POD002", "XX", 3, "hash3new"),
      etlStage3M2Model("POD002", "", "", "", "prova2", "", 202501, 1 , "POD002", "XX", 4, "hash3"),
      // for fornitura_pod POD00A table 4 nothing happen
      etlStage3M2Model("POD00A", "prova6", "", "", "", "", 202412, 25, "POD002", "XX", 5, "hash7"),
      etlStage3M2Model("POD00A", "prova7", "", "", "", "", 202411, 31, "POD002", "XX", 5, "hash8"),
      // for fornitura_pod POD00D table 3 ,competenza consumi 202501 change data,  table 3 ,competenza consumi 202403 new data
      etlStage3M2Model("POD00D", "", "prova1update", "", "", "", 202501, 1, "POD004", "XX", 3, "hash2update"),
      etlStage3M2Model("POD00D", "", "", "prova2new", "", "", 202403, 2, "POD004", "XX", 3, "hash3new"),
      // for fornitura_pod POD00C new fornitura_pod
      etlStage3M2Model("POD00C", "", "prova1", "", "", "", 202411, 1, "POD004", "XX", 2, "hash2"),
      etlStage3M2Model("POD00C", "", "", "prova2", "", "", 202411, 1, "POD004", "XX", 3, "hash3"),
      etlStage3M2Model("POD00C", "", "", "", "prova3", "", 202411, 1, "POD004", "XX", 4, "hash4"),
      etlStage3M2Model("POD00C", "", "", "", "", "prova4", 202411, 1, "POD004", "XX", 6, "hash5")
    ).toDS()
    //    stageFinalNowDS: Dataset[etlStage3M2Model]

    val etlStage3M2CompareOtherMisureDS = Seq(
      etlStage3M2Model("POD002", "", "prova1old", "", "", "", 202501, 1, "POD002", "XX", 2, "hash2old"),
      etlStage3M2Model("POD002", "", "", "", "prova2", "", 202501, 1 , "POD002", "XX", 4, "hash3"),
      // for fornitura_pod POD00A table 4 nothing happen
      etlStage3M2Model("POD00A", "prova6", "", "", "", "", 202412, 25, "POD002", "XX", 5, "hash7"),
      etlStage3M2Model("POD00A", "prova7", "", "", "", "", 202411, 31, "POD002", "XX", 5, "hash8"),
      // for fornitura_pod POD00D table 3 ,competenza consumi 202501 change data,  table 3 ,competenza consumi 202403 new data
      etlStage3M2Model("POD00D", "", "prova1old", "", "", "", 202501, 1, "POD004", "XX", 2, "hash2old")
      // for fornitura_pod POD00C new fornitura_pod
    ).toDS()

    val result = stagePhaseTrasformation.data_compare(etlStage3M2NowMisureOraraGGDS.unionByName(etlStage3M2OtherMisureGDS),etlStage3M2CompareMisureOraraGDS.unionByName(etlStage3M2CompareOtherMisureDS))
        result.show()

    val etlStage3M2ExpectedGGDS = Seq(
      // for fornitura_pod POD002 , competenze consimi 202501 , day 3 update data and hash value consequently, day 5 new data not present in previous footprint
      etlStage3M2Model("POD002", "prova1", "", "", "", "", 202501, 1, "POD002", "XX", 1, "hash2"),
      etlStage3M2Model("POD002", "prova2", "", "", "", "", 202501, 2, "POD002", "XX", 1, "hash3"),
      etlStage3M2Model("POD002", "prova3new", "", "", "", "", 202501, 3, "POD002", "XX", 1, "hash4new"),
      etlStage3M2Model("POD002", "prova4", "", "", "", "", 202501, 4, "POD002", "XX", 1, "hash5"),
      etlStage3M2Model("POD002", "prova5", "", "", "", "", 202501, 5, "POD002", "XX", 1, "hash6"),
      // for fornitura_pod POD002 , competenze consiumi 202412 nothing appen respect to previous footprint
//      etlStage3M2Model("POD002", "prova6", "", "", "", "", 202412, 25, "POD002", 1, "hash7"),
//      etlStage3M2Model("POD002", "prova7", "", "", "", "", 202412, 31, "POD002", 1, "hash8"),
      // for fornitura_pod POD003 , competenze consimi 202501 , day 2 update data and hash value consequently
      etlStage3M2Model("POD003", "prova1", "", "", "", "", 202502, 1, "POD003", "XX", 1, "hash2"),
      etlStage3M2Model("POD003", "prova2new", "", "", "", "", 202502, 2, "POD003", "XX", 1, "hash3new"),
      etlStage3M2Model("POD003", "prova3", "", "", "", "", 202502, 3, "POD003", "XX", 1, "hash4"),
      // for fornitura_pod POD004 , competenze consimi 202410 , day 4 new data
      etlStage3M2Model("POD004", "prova1", "", "", "", "", 202410, 1, "POD004", "XX", 1, "hash2"),
      etlStage3M2Model("POD004", "prova2", "", "", "", "", 202410, 2, "POD004", "XX", 1, "hash3"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 3, "POD004", "XX", 1, "hash4"),
      etlStage3M2Model("POD004", "prova3", "", "", "", "", 202410, 4, "POD004", "XX", 1, "hash4"),
      // for fornitura_pod POD005 , competenze consimi 202411 nothing happen
//      etlStage3M2Model("POD005", "prova1", "", "", "", "", 202411, 1, "POD004", 1, "hash2"),
//      etlStage3M2Model("POD005", "prova2", "", "", "", "", 202411, 2, "POD004", 1, "hash3"),
//      etlStage3M2Model("POD005", "prova3", "", "", "", "", 202411, 3, "POD004", 1, "hash4"),
//      etlStage3M2Model("POD005", "prova4", "", "", "", "", 202411, 4, "POD004", 1, "hash5"),
//      etlStage3M2Model("POD005", "prova5", "", "", "", "", 202411, 5, "POD004", 1, "hash6"),
//      etlStage3M2Model("POD005", "prova6", "", "", "", "", 202411, 6, "POD004", 1, "hash7"),
//      etlStage3M2Model("POD005", "prova7", "", "", "", "", 202411, 7, "POD004", 1, "hash8"),
//      etlStage3M2Model("POD005", "prova8", "", "", "", "", 202411, 8, "POD004", 1, "hash9"),
        // for fornitura_pod POD002, competenze consimi 202501
        // table 2 update data and hash value consequently,
        // table 3  new data not present in previous footprint
        //table 4 nothing happen
      etlStage3M2Model("POD002", "", "prova1update", "", "", "", 202501, 1, "POD002", "XX", 2, "hash2update"),
      etlStage3M2Model("POD002", "", "", "prova2new", "", "", 202501, 1, "POD002", "XX", 3, "hash3new"),
//      etlStage3M2Model("POD002", "", "", "", "prova2", "", 202501, 1 , "POD002", 4, "hash3"),
      // for fornitura_pod POD00A table 4 nothing happen
//      etlStage3M2Model("POD00A", "prova6", "", "", "", "", 202412, 25, "POD002", 5, "hash7"),
//      etlStage3M2Model("POD00A", "prova7", "", "", "", "", 202411, 31, "POD002", 5, "hash8"),
      // for fornitura_pod POD00D table 3 ,competenza consumi 202501 change data,  table 3 ,competenza consumi 202403 new data
      etlStage3M2Model("POD00D", "", "prova1update", "", "", "", 202501, 1, "POD004", "XX", 3, "hash2update"),
      etlStage3M2Model("POD00D", "", "", "prova2new", "", "", 202403, 2, "POD004", "XX", 3, "hash3new"),
      // for fornitura_pod POD00C new fornitura_pod
      etlStage3M2Model("POD00C", "", "prova1", "", "", "", 202411, 1, "POD004", "XX", 2, "hash2"),
      etlStage3M2Model("POD00C", "", "", "prova2", "", "", 202411, 1, "POD004", "XX", 3, "hash3"),
      etlStage3M2Model("POD00C", "", "", "", "prova3", "", 202411, 1, "POD004", "XX", 4, "hash4"),
      etlStage3M2Model("POD00C", "", "", "", "", "prova4", 202411, 1, "POD004", "XX", 6, "hash5")
    ).toDS()
    //    stageFinalNowDS: Dataset[etlStage3M2Model]

    def applyTableFilter(df : Dataset[etlStage3M2Model],tableValue: Int): Long = {
      df.filter(col(etlStage3M2Schema.tabella) === tableValue).count()
    }

    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,1),applyTableFilter(result,1) )
    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,2),applyTableFilter(result,2) )
    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,3),applyTableFilter(result,3) )
    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,4),applyTableFilter(result,4) )
    Assert.assertEquals(applyTableFilter(etlStage3M2ExpectedGGDS,5),applyTableFilter(result,5) )



  }

   @Ignore
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
//    Assert.assertFalse(stagePhaseTrasformation.controlloEtlPrecedente(etlStage3M2NotOkCountDS,registroLoadDS))
//    Assert.assertFalse(stagePhaseTrasformation.controlloEtlPrecedente(etlStage3M2NotOkLastCodPodDS,registroLoadDS))
  }

}
