package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricF2Model, misureStoricF2ErcEriModel, misureStoricNoraErcEriModel}
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.trasformations.{fornitureElettricheTmpTrasfornation, misureStoricoErcEriTrasformation}
import org.apache.spark.sql.SparkSession

class testMisureStoricoErcEriTrasfornation extends EnvironmentSparkTest{


  implicit val spark: SparkSession = EnvironmentMisure.getSpark  // Make it implicit
  import spark.implicits._

  def testMisureStoricNoraF2ErcEriPrepared():Unit ={
    val dsForniture = Seq(FornitureElettricheTmpModel()).toDS()
    val dsMisureStoricNoraErcEri = Seq(misureStoricNoraErcEriModel()).toDS()

    val dsFornitureTmp = misureStoricoErcEriTrasformation.misureStoricNoraF2ErcEriPrepared(dsMisureStoricNoraErcEri,dsForniture)

  }

  def testConsultazione():Unit ={
    val dsMisureStoricF2 = Seq(MisureStoricF2Model()).toDS()
    val dsMisureStoricF2ErcEri = Seq(misureStoricF2ErcEriModel()).toDS()

    val dsFornitureTmp = misureStoricoErcEriTrasformation.consultazioneDefinition(dsMisureStoricF2,dsMisureStoricF2ErcEri)

  }


}
