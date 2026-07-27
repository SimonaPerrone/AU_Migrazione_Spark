package it.eng.au.portale_consumi_ee.schema.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum

object etlStage3M2ProposedSchema extends SchemaEnum{
 val
         n_id_fornitura,
         misure_orarie,
         misure_mensili,
         misure_non_orarie,
         volture,
         autoletture,
         pod,
         cod_pod,
         hash_value,
         last_update,
         competenza_consumi
 = Value
}