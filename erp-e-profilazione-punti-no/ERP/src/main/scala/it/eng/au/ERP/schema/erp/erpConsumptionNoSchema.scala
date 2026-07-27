package it.eng.au.ERP.schema.erp

import it.eng.au.ERP.schema.SchemaEnum

object erpConsumptionNoSchema extends SchemaEnum {
   //todo set order based on column order defined by hive table
  val
     pod,
     trattamento,
     annomese,
     stato,
     data_misura_sx,
     tipo_flusso_sx,
     nomefile_sx,
     eam_sx,
     eaf1_sx,
     eaf2_sx,
     eaf3_sx,
     data_misura_dx,
     tipo_flusso_dx,
     nomefile_dx,
     eam_dx,
     eaf1_dx,
     eaf2_dx,
     eaf3_dx,
     c_eam,
     c_eaf1,
     c_eaf2,
     c_eaf3,
     executionid
  = Value
}
