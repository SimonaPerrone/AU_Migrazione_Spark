package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object GdmSchema extends SchemaEnum{
  val
          n_id_pod,
          codice_pod,
          n_potenza_disponibile,
          n_potenza_impegnata,
          n_tensione,
          t_tipo_misuratore,
          d_oper_misurator_att,
          d_oper_misurator_att_str,
          cambio_gdm,
          data_cambio_gdm,
          data_cambio_gdm_str,
          trattamento,
          stato_misuratore_2g,
          t_mat_misuratore_att,
          d_inst_misurator_att,
          anno_start_misure_orarie,
          mese_start_misure_orarie
  = Value
}
