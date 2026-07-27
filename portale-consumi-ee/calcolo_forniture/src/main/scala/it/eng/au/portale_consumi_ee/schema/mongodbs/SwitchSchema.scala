package it.eng.au.portale_consumi_ee.schema.mongodbs

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object SwitchSchema extends SchemaEnum{
  val
                  t_codice_pod,
                  data_switch,
                  n_id_pratica,
                  switching_in_corso,
                  n_id_cliente
  = Value
}
