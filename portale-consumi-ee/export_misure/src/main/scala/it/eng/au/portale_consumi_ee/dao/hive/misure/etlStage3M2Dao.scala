package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2Model, voltureModel}
import it.eng.au.portale_consumi_ee.schema.misure.{etlStage3M2Schema, voltureSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class etlStage3M2Dao extends HiveMisureDao[etlStage3M2Model]{

  override val tableName: String = PropertyUtility.etldStageg3M2Table
  override val schema: SchemaEnum = etlStage3M2Schema
}
