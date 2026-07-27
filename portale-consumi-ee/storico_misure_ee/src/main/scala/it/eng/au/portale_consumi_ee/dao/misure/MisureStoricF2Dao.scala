package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricF2Model, MisureStoricFModel}
import it.eng.au.portale_consumi_ee.schema.misure.{MisureStoricF2Schema, MisureStoricFSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class MisureStoricF2Dao extends HiveMisureDao[MisureStoricF2Model]{
  override val tableName: String = PropertyUtility.misureStoricF2
  override val schema: SchemaEnum = MisureStoricF2Schema

}