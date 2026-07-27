package it.eng.au.portale_consumi_ee.dao.userappl

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.userappl.UserapplT001AppPrtPratichePModel
import it.eng.au.portale_consumi_ee.schema.userappl.UserapplT001AppPrtPratichePSchema

case class UserapplT001AppPrtPratichePDao() extends HiveDao[UserapplT001AppPrtPratichePModel]{

  override val tableName: String = Environment.getProperty("hive.table.userappl.t001_app_prt_pratiche_p")
  override val schema: SchemaEnum = UserapplT001AppPrtPratichePSchema

}
