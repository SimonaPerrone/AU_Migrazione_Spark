package it.eng.au.portale_consumi_ee.schema.userappl

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object UserapplT001AppPrtPratichePSchema extends SchemaEnum {
  val
              n_id_pratica,
              t_protocollo,
              n_id_descrittore_processo,
              t_stato,
              n_id_utente,
              n_id_operatore,
              d_data_apertura,
              d_data_chiusura,
              t_pod,
              n_contatore_modifica,
              t_stato_business,
              t_archiviata,
              n_id_utente_modifica,
              n_id_pratica_origine,
              n_id_operatore_chiusura,
              t_visibile,
              t_url_annull
  = Value
}
