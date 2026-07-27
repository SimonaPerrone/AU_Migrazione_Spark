package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricF2Model, MisureStoricModel, MisureStoricNoraModel, misureStoricF2ErcEriModel}
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.trasformations.{misureStoricoErcEriTrasformation, misureStoricoTrasformation}
import org.apache.spark.sql.SparkSession

class testMisureStoricoErcEriTrasformation extends EnvironmentSparkTest{


  implicit val spark: SparkSession = EnvironmentMisure.getSpark  // Make it implicit
  import spark.implicits._

  def testConsultazione():Unit ={
    val dsMisureStoricF2 = Seq(
      MisureStoricF2Model(
        cf_piva = "ABC123DEF45",
        pod = "IT001E00000001",
        data_lettura = "2025-04-01",
        data_ricezione = "2025-04-02",
        motivazione = "LETTURA PERIODICA",
        lettura_monoraria = 1234.56,
        lettura_f1 = 456.78,
        lettura_f2 = 567.89,
        lettura_f3 = 210.12,
        lettura_f4 = 0.0,
        lettura_f5 = 0.0,
        lettura_f6 = 0.0,
        ea = "EA001",
        er = "ER001",
        tipo_flusso = "M1",
        data_lettura_num = 20250401L,
        annomese_riferimento = 202504,
        cod_pod = "CODPOD001",
        is_mis_oraria = "S"
      ),
      MisureStoricF2Model(
        cf_piva = "ABC123DEF45",
        pod = "IT001E00000001ZZZ",
        data_lettura = "2025-04-01",
        data_ricezione = "2025-04-02",
        motivazione = "LETTURA PERIODICA",
        lettura_monoraria = 1234.56,
        lettura_f1 = 456.78,
        lettura_f2 = 567.89,
        lettura_f3 = 210.12,
        lettura_f4 = 0.0,
        lettura_f5 = 0.0,
        lettura_f6 = 0.0,
        ea = "EA001",
        er = "ER001",
        tipo_flusso = "M1",
        data_lettura_num = 20250401L,
        annomese_riferimento = 202504,
        cod_pod = "CODPOD001",
        is_mis_oraria = "S")
    ).toDS()
    val dsMisureStoricF2ErcEri = Seq(
      misureStoricF2ErcEriModel(
        cf_piva = "XYZ789LMN12",
        pod = "IT001E00000001",
        data_lettura_str = "2025-04-01",
        data_ricezione = "2025-04-02",
        lettura_erc_f1 = 321.0,
        lettura_erc_f2 = 432.1,
        lettura_erc_f3 = 543.2,
        lettura_erc_f4 = 0.0,
        lettura_erc_f5 = 0.0,
        lettura_erc_f6 = 0.0,
        lettura_eri_f1 = 111.0,
        lettura_eri_f2 = 222.0,
        lettura_eri_f3 = 333.0,
        lettura_eri_f4 = 0.0,
        lettura_eri_f5 = 0.0,
        lettura_eri_f6 = 0.0,
        erc = "ERC002",
        eri = "ERI002",
        tipo_flusso = "M1",
        annomese_riferimento = 202504,
        data_lettura = 20250401L,
        cod_pod = "CD",
        is_mis_oraria = "N"
      )
    ).toDS()


    val dsFornitureTmp = misureStoricoErcEriTrasformation.consultazioneDefinition(dsMisureStoricF2,dsMisureStoricF2ErcEri)

    dsFornitureTmp.show()
  }

  def testStoricF2Prepare():Unit ={
    val dsMisureStoricF2 = Seq(MisureStoricF2Model()).toDS()
    val annomese = 202504
    val is_mis_oraria = "1"

    val dsFornitureTmp = misureStoricoErcEriTrasformation.misureStoricF2Prepared(dsMisureStoricF2,annomese)

  }

  def testStoricF2ErcEriPrepare():Unit ={
    val dsMisureStoricF2ErcEri = Seq(misureStoricF2ErcEriModel()).toDS()
    val annomese = 202504
    val is_mis_oraria = "1"

    val dsFornitureTmp = misureStoricoErcEriTrasformation.misureStoricF2ErcEriPrepared(dsMisureStoricF2ErcEri,annomese)

  }


}
