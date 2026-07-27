package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2Model, etlStage3M2ProposedModel}
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2ProposedSchema, etlStage3M2Schema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility
import org.apache.spark.sql.Dataset

class etlStage3M2RevisitedOnHiveDao extends HiveMisureDao[etlStage3M2ProposedModel]{

  override val tableName: String = PropertyUtility.etldStageg3M2Table
  override val schema: SchemaEnum = etlStage3M2ProposedSchema

}
