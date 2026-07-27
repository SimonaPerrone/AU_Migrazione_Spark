package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricModel, MisureStoricNoraModel}
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.trasformations.{fornitureElettricheTmpTrasfornation, misureStoricoTrasformation}
import org.apache.spark.sql.SparkSession

class testMisureStoricoTrasformation extends EnvironmentSparkTest{


  implicit val spark: SparkSession = EnvironmentMisure.getSpark  // Make it implicit
  import spark.implicits._

  def testStornicoNora():Unit ={
    val dsForniture = Seq(FornitureElettricheTmpModel()).toDS()
    val dsStoricoNora = Seq(MisureStoricNoraModel()).toDS()

    val dsFornitureTmp = misureStoricoTrasformation.misureStoricNoraDefinition(dsStoricoNora,dsForniture)

  }

  def testStornicoOra():Unit ={
    val dsForniture = Seq(FornitureElettricheTmpModel()).toDS()
    val dsStoricoOra = Seq(MisureStoricModel()).toDS()

    val dsFornitureTmp = misureStoricoTrasformation.misureStoricOraDefinition(dsStoricoOra,dsForniture)

  }

}
