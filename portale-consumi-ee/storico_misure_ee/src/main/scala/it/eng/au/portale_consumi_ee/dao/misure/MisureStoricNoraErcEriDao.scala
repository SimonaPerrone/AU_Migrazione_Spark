package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{misureStoricF2ErcEriModel, misureStoricNoraErcEriModel}
import it.eng.au.portale_consumi_ee.schema.misure.{misureStoricF2ErcEriSchema, misureStoricNoraErcEriSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class MisureStoricNoraErcEriDao extends HiveMisureDao[misureStoricNoraErcEriModel]{
  override val tableName: String = PropertyUtility.misureStoricNoraErcEri
  override val schema: SchemaEnum = misureStoricNoraErcEriSchema

}