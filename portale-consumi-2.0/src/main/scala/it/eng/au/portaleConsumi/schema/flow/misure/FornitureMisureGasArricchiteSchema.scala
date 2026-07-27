package it.eng.au.portaleConsumi.schema.flow.misure

import it.eng.au.portaleConsumi.schema.SchemaEnum

object FornitureMisureGasArricchiteSchema extends SchemaEnum {

  val
  codice_fiscale,
  codice_pdr,
  codice_fornitura,
  lettura,
  flusso,
  gruppo_flusso,
  data_lettura,
  data_caricamento,
  motivazione,
  delta_misure,
  usata_per_calcolo, //se usata per calcolare delta
  riempimento, // 0 misura letta, 1 fill inizio fornitura, 2 calcolo mensile da giornaliero
  cod_pdr, // sottostringa del codice pdr usata per la storic_f2
  data_calcolo,
  annomese
  = Value
}
