package it.eng.au.ERP.dao.hive.tratt_pod

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.model.tratt_pod.TrattPodAnnomesePartitionedModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.rcu.RcuAziendaPSchema
import it.eng.au.ERP.schema.tratt_pod.trattPodAllAnnomesePartitionedSchema
import it.eng.au.ERP.utility.setting.PropertyUtility

class TrattPodllAnnomesePartitionedDao extends HiveDao[TrattPodAnnomesePartitionedModel]{
  override val tableName: String = PropertyUtility.trattPodAllAnnomesePartitined
  override val schema: SchemaEnum = trattPodAllAnnomesePartitionedSchema
}
