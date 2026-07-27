package it.eng.au.export_misure.storico

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.common.dao.mongodbs.fornitureElettricheDao
import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.trasformations.fornitureElettricheTmpTrasfornation
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilities
import org.apache.spark.sql.SparkSession

class testFornitureElettricheTmpTrasfornation extends EnvironmentSparkTest{


  implicit val spark: SparkSession = EnvironmentMisure.getSpark  // Make it implicit
  import spark.implicits._

  def testFornitureElettricheTmpTrasfornation():Unit ={
    val dsForniture = Seq(fornitureElettricheModel()).toDS()

    val dsFornitureTmp = fornitureElettricheTmpTrasfornation.fornitureElettricheTmp(dsForniture)

  }


}
