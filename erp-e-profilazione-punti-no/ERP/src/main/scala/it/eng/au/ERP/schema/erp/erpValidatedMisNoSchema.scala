package it.eng.au.ERP.schema.erp

import it.eng.au.ERP.schema.SchemaEnum

object erpValidatedMisNoSchema extends SchemaEnum {
   // Order matches Hive table column order (CRITICAL for insertInto!)
  val
     pod,
     anno,
     mese,
     data_misura,
     tipo_dato_s,
     tipo_dato_e,
     time_stamp,
     eam,
     eaf1,
     eaf2,
     eaf3,
     tipo_flusso,
     nomefile,
     coeff_perdita,
     k,
     trattamento,
     executionid
  = Value
}
