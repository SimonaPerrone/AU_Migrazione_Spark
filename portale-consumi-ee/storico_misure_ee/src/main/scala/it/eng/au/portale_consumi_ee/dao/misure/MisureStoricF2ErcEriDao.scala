package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricModel, misureStoricF2ErcEriModel}
import it.eng.au.portale_consumi_ee.schema.misure.{MisureStoricSchema, misureStoricF2ErcEriSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class MisureStoricF2ErcEriDao extends HiveMisureDao[misureStoricF2ErcEriModel]{
  override val tableName: String = PropertyUtility.misureStoricF2ErcEri
  override val schema: SchemaEnum = misureStoricF2ErcEriSchema

}