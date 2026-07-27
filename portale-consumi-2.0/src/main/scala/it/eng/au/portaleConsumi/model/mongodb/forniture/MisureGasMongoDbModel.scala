package it.eng.au.portaleConsumi.model.mongodb.forniture

import it.eng.au.portaleConsumi.utility.common.PrintHelper

// classe per leggere le misure in stile JSON dato che non si puo' ricondurre la struttura Mongo ad una classe
// fissa in quanto le misure possono non essere sempre presenti e da errore
case class MisureJson(_id: String, codice_fornitura: String, pdr: String, misure: String)

case class MisureGasMongoDbModel(_id: String, codice_fornitura: String, pdr: String, misure: Misura){
  override def toString: String = {
    s"_id: ${_id}, codice_fornitura: $codice_fornitura, pdr: $pdr, misure: $misure"
  }
}

case class Misura(
                   autoletture: Array[AutoletturaDettaglio] = null,
                   misure_giornaliere: Array[GiornalieraDettaglio] = null,
                   misure_altre_frequenze: Array[MensileDettaglio] = null,
                   misure_mensili: Array[MensileDettaglio] = null,
                   volture: Array[VolturaDettaglio] = null
                 ) {
  override def toString: String = {
    s"autoletture: [${if (autoletture == null) null else PrintHelper.attributeToString(autoletture.toList.sortWith((f, s) => f.data_lettura < s.data_lettura))}}], " +
      s"misure_giornaliere:  [${if (misure_giornaliere == null) "" else PrintHelper.attributeToString(misure_giornaliere.toList.sortWith((f, s) => f.data_lettura < s.data_lettura))}], " +
      s"misure_altre_frequenze:  [${if (misure_altre_frequenze == null) null else  PrintHelper.attributeToString(misure_altre_frequenze.toList.sortWith((f, s) => f.data_lettura < s.data_lettura))}}], " +
      s"misure_mensili:  [${if (misure_mensili == null) null else PrintHelper.attributeToString(misure_mensili.toList.sortWith((f, s) => f.data_lettura < s.data_lettura))}}], " +
      s"volture:  [${if (volture == null) null else PrintHelper.attributeToString(volture.toList.sortWith((f, s) => f.data_lettura < s.data_lettura))}}] "
  }
}

case class MensileDettaglio(competenza_consumi: String, data_lettura: String, delta_misure: String, lettura_mese: String, tipo_misura: String)

case class VolturaDettaglio(competenza_consumi: String, data_lettura: String, lettura_misura: String, tipo_misura: String)

case class AutoletturaDettaglio(competenza_consumi: String, data_lettura: String, lettura_mese: String, tipo_misura: String)

case class GiornalieraDettaglio(competenza_consumi: String, data_lettura: String, delta_misure: String, lettura_giorno: String, tipo_misura: String)

// Classi di supporto per conversione a struttura finale
case class MisuraDettaglioEstesa(codice_fornitura: String, pdr: String, autoletture: AutoletturaDettaglio,
                                 misure_giornaliere: GiornalieraDettaglio, misure_altre_frequenze: MensileDettaglio,
                                 misure_mensili: MensileDettaglio, volture: VolturaDettaglio)
case class MisuraDettaglio(codice_fornitura: String, pdr: String, autoletture: Array[AutoletturaDettaglio],
                                 misure_giornaliere: Array[GiornalieraDettaglio], misure_altre_frequenze: Array[MensileDettaglio],
                                 misure_mensili: Array[MensileDettaglio], volture: Array[VolturaDettaglio])
