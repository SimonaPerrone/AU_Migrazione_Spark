package it.eng.au.portale_consumi_ee.model.Forniture

import it.eng.au.portale_consumi_ee.model.misure._

case class ProvaModel(
                         _id: String = null,
                         codice_fornitura: String = null,
                         pod: String = null,
                         misura_oraria_gg: List[misureOrarieCStructValues] = null,
                         misura_oraria_mese: misureMensiliCStructValues = null,
                         misura_non_oraria: misureNonOrarieCStructValues = null,
                         volture:VoltureValues = null,
                         autolettura:AutoletturaValues = null
                              )
