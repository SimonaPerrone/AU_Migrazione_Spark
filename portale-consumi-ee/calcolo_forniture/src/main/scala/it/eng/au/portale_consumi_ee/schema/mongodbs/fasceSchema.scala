package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object fasceSchema extends SchemaEnum{
  val
                  n_id_pod,
                  n_id_misuratore,
                  f_lunedi,
                  f_martedi,
                  f_mercoledi,
                  f_giovedi,
                  f_venerdi,
                  f_sabato,
                  f_domenica,
                  f_festivo,
                  d_inizio_validita,
                  d_fine_validita,
                  d_fine_validita_str,
                  d_data_iniziofreezing
  = Value
}
