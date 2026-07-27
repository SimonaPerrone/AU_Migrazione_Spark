package it.eng.au.portale_consumi_ee.model.misure

case class etlStage3M2ProposedModel(
                                     n_id_fornitura: String = null,
                                     misure_orarie: List[misureOrarieCStructValues] = null,
                                     misure_mensili: misureMensiliCStructValues = null,
                                     misure_non_orarie: misureNonOrarieCStructValues = null,
                                     volture:VoltureValues = null,
                                     autoletture:AutoletturaValues = null,
                                     pod: String = null,
                                     cod_pod: String = null,
                                     hash_value: String = null,
                                     last_update: java.lang.Long = null,
                                     competenza_consumi: java.lang.Integer = null
                                   )
