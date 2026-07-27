package it.eng.au.ERP.dao.hive.rcu

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.rcu.{RcuAziendaPModel, RcuPodInterconnesionePModel}
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.rcu.{RcuAziendaPSchema, RcuPodInterconnesionePSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility

class RcuPodInterconnesionePDao extends HiveDao[RcuPodInterconnesionePModel]{
  override val tableName: String = PropertyUtility.rcuPodInterconnesioneP
  override val schema: SchemaEnum = RcuPodInterconnesionePSchema
}
