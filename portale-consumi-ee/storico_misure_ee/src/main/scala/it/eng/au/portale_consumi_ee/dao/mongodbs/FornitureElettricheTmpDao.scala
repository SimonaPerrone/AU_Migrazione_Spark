package it.eng.au.portale_consumi_ee.dao.mongodbs

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

 class FornitureElettricheTmpDao extends HiveMisureDao[FornitureElettricheTmpModel]{
   override val tableName: String = PropertyUtility.fornitureElettricheTmp
   override val schema: SchemaEnum = FornitureElettricheTmpSchema

 }