package it.eng.au.ccgPubblicazione.controller.sessionfactory.traits

import it.eng.au.ccgPubblicazione.schema.cdp.{CaFinalCdpSchema, CdpConsumptionRequestRunnableSchema, ValidatedFlowsCdpSchema}

trait CdpFinRicFlow extends SessionRun {
  override val fieldsConsumptionRequestRunnable: List[String] = CdpConsumptionRequestRunnableSchema.getValues
  override val pivaUddFieldConsumption: String = CaFinalCdpSchema.piva_udd
  override val pivaUdbFieldConsumption: String = CaFinalCdpSchema.piva_udb
  override val pivaIdFieldConsumption: String = CaFinalCdpSchema.piva_distr
  override val pivaGestoreFieldConsumption: String = CdpConsumptionRequestRunnableSchema.pivaGestore
  override val pdrFieldConsumption: String = CaFinalCdpSchema.codice_pdr
  override val pdrFieldValidation: String = ValidatedFlowsCdpSchema.pdr
  override val idRichiestaFields: String = CdpConsumptionRequestRunnableSchema.idRichiesta
//  override val dataRichiestaFields: String = CdpConsumptionRequestRunnableSchema.dataRichiesta
  override val filtroFieldCodProfConsumption: String = CaFinalCdpSchema.cod_prof_prel_std
  override val filtroFiledCodRemiConsumption: String = CaFinalCdpSchema.codice_remi
  override val filtroFiledTrattamentoConsumption: String = CaFinalCdpSchema.trattamento

}
