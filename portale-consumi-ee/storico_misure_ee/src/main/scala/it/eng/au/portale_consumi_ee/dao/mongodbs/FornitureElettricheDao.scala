package it.eng.au.portale_consumi_ee.dao.mongodbs


import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao

import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class FornitureElettricheDao extends HiveMisureDao[fornitureElettricheModel]{
  override val tableName: String = PropertyUtility.fornitureElettriche
  override val schema: SchemaEnum = fornitureElettricheSchema

}