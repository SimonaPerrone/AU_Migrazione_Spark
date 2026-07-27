package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.schema.misure.etlStage3M2ProposedSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class etlStage33M2RevisitedOnHiveDao extends HiveMisureDao[etlStage3M2ProposedModel]{

  override val tableName: String = PropertyUtility.etldStageg33M2Table
  override val schema: SchemaEnum = etlStage3M2ProposedSchema
}
