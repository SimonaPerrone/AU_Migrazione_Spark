package it.au.misure.ingestionMisureGasUnico.model.schema.ammissibilita

import it.au.misure.ingestionMisureGasUnico.model.schema.SchemaEnum

object AmmissibilitaFileSchema extends SchemaEnum{
  val
  cartella_cloud,
  nome_file,
  flusso,
  ammissibilita,
  bloccante,
  codice_inamissibilita,
  descrizione,
  d_caricamento,
  anno,
  mese,
  giorno
  = Value
}
