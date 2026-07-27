package it.eng.au.portaleConsumi.model.mongodb.forniture

import it.eng.au.portaleConsumi.utility.common.PrintHelper

// Classe completa che rappresenta il dato finale presente in MongoDB
case class FornitureGasMongoDbModel(_id: String, anagrafica: Anagrafica, codice_fiscale: String, id: String, pdr: Array[Pdr]) {
  override def toString: String = {
    s"FornitureGasMongoDbModel(${_id}, $anagrafica, $codice_fiscale, $id, pdr: [${
      PrintHelper.attributeToString(
        pdr.toList.sortWith((f, s) => f.codice_pdr < s.codice_pdr))
    }}])"
  }

  def toStringNoIds: String = {
    s"FornitureGasMongoDbModel($anagrafica, $codice_fiscale, pdr: [${
      PrintHelper.attributeToString(
        pdr.toList.sortWith((f, s) => f.codice_pdr < s.codice_pdr))
    }}])"
  }
}

// Dati elemento anagrafica
case class Anagrafica(nome: String, cognome: String, p_iva: String, ragione_sociale: String)

// Dati relativi al PDR
case class Pdr(codice_pdr: String, forniture: Array[Fornitura], processi: Array[Processo]) {
  override def toString: String = {
    s"Pdr($codice_pdr, " +
      s"forniture: [${
        PrintHelper.attributeToString(forniture.toList.sortWith((f, s) => f.codice_fornitura < s.codice_fornitura ||
          f.data_inizio_fornitura < s.data_inizio_fornitura))
      }], " +
      s"processi: [${PrintHelper.attributeToString(processi.toList.sortWith((f, s) => f.tipo_processo < s.tipo_processo))}])"
  }
}

// Dati elemento fornitura
case class Fornitura(data_inizio_fornitura: String, data_fine_fornitura: String, codice_fornitura: String, cap: String,
                     categoria_uso: String, civico: String, classe_misuratore: String, coefficiente_conversione: String,
                     comune: String, matricola_misuratore: String, nazione: String, nome_strada: String,
                     p_iva_cc: String, provincia: String, ragione_sociale_cc: String, ragione_sociale_distributore: String,
                     residente: String, tipo_fornitura: String, tipo_pdr: String, toponimo_Indirizzo: String,
                    // AU-734
                     codice_offerta: String,
                     cliente_vulnerabile: String // dominio: Y-N
                    )
// Dati processo
case class Processo(id_processo: String, data_inizio_processo: String, data_fine_processo: String,
                    data_di_decorrenza: String, in_corso: String, note: String, tipo_processo: String)

// Classi di supporto per conversione a struttura finale con i raggruppamenti progressivi richiesti
// Raggruppamento processi
case class FornituraProcessi(codice_fiscale: String, codice_pdr: String, anagrafica: Anagrafica, fornitura: Fornitura, processi: Array[Processo])

// Raggruppamento forniture
case class FornitureProcessi(codice_fiscale: String, codice_pdr: String, anagrafica: Anagrafica, forniture: Array[Fornitura], processi: Array[Processo])

// Raggruppamento PDR
case class PdrProcessi(codice_fiscale: String, anagrafica: Anagrafica, pdr: Pdr)
