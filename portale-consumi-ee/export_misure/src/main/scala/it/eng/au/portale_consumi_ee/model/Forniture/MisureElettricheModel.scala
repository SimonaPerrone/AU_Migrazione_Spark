package it.eng.au.portale_consumi_ee.model.Forniture

import it.eng.au.portale_consumi_ee.model.misure.{AutoletturaValues, VoltureValues, misureMensiliCStructValues, misureNonOrarieCStructValues, misureOrarieCStructValues}

case class MisureElettricheModel(
                                  _id: String = null,
                                  codice_fornitura: String = null,
                                  competenza_consumi: java.lang.Integer = null,
                                  pod: String = null,
                                  misure_orarie: List[misureOrarieCStructValues] = null,
                                  misure_mensili: misureMensiliCStructValues = null,
                                  misure_non_orarie: misureNonOrarieCStructValues = null,
                                  volture:VoltureValues = null,
                                  autoletture:AutoletturaValues = null
                              )
