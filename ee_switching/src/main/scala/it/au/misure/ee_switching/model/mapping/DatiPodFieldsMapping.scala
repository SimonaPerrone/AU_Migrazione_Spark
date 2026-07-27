package it.au.misure.ee_switching.model.mapping

import it.au.misure.ee_switching.model.schema.hive.{FunzionaliCompressedSchema, StoriciCompressedSchema}
import it.au.misure.ee_switching.model.schema.xml.{FunzionaliXMLSchema, StoriciXMLSchema, TagXml}

import scala.collection.SortedMap

object DatiPodFieldsMapping {

  val mappingDatiPodFunzionali: SortedMap[Int, TagXml] = SortedMap(
      1 -> TagXml(tagName = FunzionaliXMLSchema.DatiPod, onlyOpening = true),
      2 -> TagXml(tagName = FunzionaliXMLSchema.Pod, infoFrom = FunzionaliCompressedSchema.pod14),
      3 -> TagXml(tagName = FunzionaliXMLSchema.CodPrat_SII, infoFrom = FunzionaliCompressedSchema.t_protocollo),
      4 -> TagXml(tagName = FunzionaliXMLSchema.DataInizio, infoFrom = FunzionaliCompressedSchema.d_data_decorrenza, toItalianDate = true),
      5 -> TagXml(tagName = FunzionaliXMLSchema.DatiPdp, onlyOpening = true),
      6 -> TagXml(tagName = FunzionaliXMLSchema.PuntoDispacciamento, infoFrom = FunzionaliCompressedSchema.t_area_rif),
      7 -> TagXml(tagName = FunzionaliXMLSchema.TipoMisuratore, infoFrom = FunzionaliCompressedSchema.t_tipo_misuratore),
      8 -> TagXml(tagName = FunzionaliXMLSchema.DataMessaRegime2G, infoFrom = FunzionaliCompressedSchema.d_regime, toItalianDate = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "F2G")),
      9 -> TagXml(tagName = FunzionaliXMLSchema.Trattamento, infoFrom = FunzionaliCompressedSchema.trattamento_online),
      10 -> TagXml(tagName = FunzionaliXMLSchema.Tensione, infoFrom = FunzionaliCompressedSchema.n_tensione, doubleToInt = true),
      11 -> TagXml(tagName = FunzionaliXMLSchema.PotImp, infoFrom = FunzionaliCompressedSchema.n_potenza_impegnata, formatDouble = true),
      12 -> TagXml(tagName = FunzionaliXMLSchema.PotDisp, infoFrom = FunzionaliCompressedSchema.n_potenza_disponibile, formatDouble = true),
      13 -> TagXml(tagName = FunzionaliXMLSchema.Ka, infoFrom = FunzionaliCompressedSchema.n_k_trasfor_att, formatDouble = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      14 -> TagXml(tagName = FunzionaliXMLSchema.Kr, infoFrom = FunzionaliCompressedSchema.n_k_trasfor_rea, formatDouble = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      15 -> TagXml(tagName = FunzionaliXMLSchema.Kp, infoFrom = FunzionaliCompressedSchema.n_k_trasfor_pot, formatDouble = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      16 -> TagXml(tagName = FunzionaliXMLSchema.MatrAtt, infoFrom = FunzionaliCompressedSchema.t_mat_misuratore_att, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      17 -> TagXml(tagName = FunzionaliXMLSchema.MatrRea, infoFrom = FunzionaliCompressedSchema.t_mat_misuratore_rea, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      18 -> TagXml(tagName = FunzionaliXMLSchema.MatrPot, infoFrom = FunzionaliCompressedSchema.t_mat_misuratore_pot, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      19 -> TagXml(tagName = FunzionaliXMLSchema.DataInstMisAtt, infoFrom = FunzionaliCompressedSchema.d_inst_misurator_att, toItalianDate = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      20 -> TagXml(tagName = FunzionaliXMLSchema.DataInstMisRea, infoFrom = FunzionaliCompressedSchema.d_inst_misurator_rea, toItalianDate = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      21 -> TagXml(tagName = FunzionaliXMLSchema.DataInstMisPot, infoFrom = FunzionaliCompressedSchema.d_inst_misurator_pot, toItalianDate = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      22 -> TagXml(tagName = FunzionaliXMLSchema.CifreAtt, infoFrom = FunzionaliCompressedSchema.n_num_cifre_att, doubleToInt = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      23 -> TagXml(tagName = FunzionaliXMLSchema.CifreRea, infoFrom = FunzionaliCompressedSchema.n_num_cifre_rea, doubleToInt = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      24 -> TagXml(tagName = FunzionaliXMLSchema.CifrePot, infoFrom = FunzionaliCompressedSchema.n_num_cifre_pot, doubleToInt = true, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "SNF" || podMeta.nomeFlusso == "F2G")),
      25 -> TagXml(tagName = FunzionaliXMLSchema.GruppoMis, infoFrom = FunzionaliCompressedSchema.b_presenza_mis),
      26 -> TagXml(tagName = FunzionaliXMLSchema.Forfait, infoFrom = FunzionaliCompressedSchema.b_gest_forfait),
      27 -> TagXml(tagName = FunzionaliXMLSchema.CodiceTariffa, infoFrom = FunzionaliCompressedSchema.t_tariffa_distr),
      28 -> TagXml(tagName = FunzionaliXMLSchema.Residenza, infoFrom = FunzionaliCompressedSchema.t_residente),
      29 -> TagXml(tagName = FunzionaliXMLSchema.Disaliment, infoFrom = FunzionaliCompressedSchema.b_disalimentabilita),
      30 -> TagXml(tagName = FunzionaliXMLSchema.ServizioTutela, infoFrom = FunzionaliCompressedSchema.servizio_tutela),
      31 -> TagXml(tagName = FunzionaliXMLSchema.ConfMis, infoFrom = FunzionaliCompressedSchema.t_tipo_configurazione),
      32 -> TagXml(tagName = FunzionaliXMLSchema.DatiPdp, onlyClosure = true),
      33 -> TagXml(tagName = FunzionaliXMLSchema.DatiPod, onlyClosure = true)
    )

  val mappingDatiPodStorici: SortedMap[Int, TagXml] = SortedMap(
    1 -> TagXml(tagName = StoriciXMLSchema.DatiPod, onlyOpening = true),
    2 -> TagXml(tagName = StoriciXMLSchema.Pod, infoFrom = StoriciCompressedSchema.pod14),
    3 -> TagXml(tagName = StoriciXMLSchema.MeseAnno, infoFrom = StoriciCompressedSchema.data_misura, stringTransformation = (dataMisura) => (dataMisura.substring(3))),
    4 -> TagXml(tagName = StoriciXMLSchema.DatiPdp, onlyOpening = true),
    5 -> TagXml(tagName = StoriciXMLSchema.MessaRegime, infoFrom = StoriciCompressedSchema.messa_regime, presenceCondition = (podMeta) => (podMeta.nomeFlusso == "S2G")),
    6 -> TagXml(tagName = StoriciXMLSchema.Trattamento, infoFrom = StoriciCompressedSchema.trattamento, nvlDefaultValue = "O"),
    7 -> TagXml(tagName = StoriciXMLSchema.TipoMisuratore, infoFrom = StoriciCompressedSchema.tipo_misuratore),
    8 -> TagXml(tagName = StoriciXMLSchema.DatiPdp, onlyClosure = true),
    9 -> TagXml(tagName = StoriciXMLSchema.Consumo, onlyOpening = true),
    10 -> TagXml(tagName = StoriciXMLSchema.TipoDato, infoFrom = StoriciCompressedSchema.tipo_dato),
    11 -> TagXml(tagName = StoriciXMLSchema.PotMax, infoFrom = StoriciCompressedSchema.last_potmax, formatDouble = true, presenceCondition = (podMeta) => (podMeta.trattamento == "O" || (podMeta.tipoMisuratore == "G" && podMeta.messaRegime == "SI"))),
    12 -> TagXml(tagName = StoriciXMLSchema.Ea, infoFrom = StoriciXMLSchema.Ea, readyTag = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || podMeta.trattamento == "O")),
    13 -> TagXml(tagName = StoriciXMLSchema.EaM, infoFrom = StoriciCompressedSchema.somma_eam, formatDouble = true, onlyPositiveValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore != "G" && podMeta.trattamento == "M")),
    14 -> TagXml(tagName = StoriciXMLSchema.EaF1, infoFrom = StoriciCompressedSchema.somma_eaf1, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    15 -> TagXml(tagName = StoriciXMLSchema.EaF2, infoFrom = StoriciCompressedSchema.somma_eaf2, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    16 -> TagXml(tagName = StoriciXMLSchema.EaF3, infoFrom = StoriciCompressedSchema.somma_eaf3, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    17 -> TagXml(tagName = StoriciXMLSchema.EaF4, infoFrom = StoriciCompressedSchema.somma_eaf4, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    18 -> TagXml(tagName = StoriciXMLSchema.EaF5, infoFrom = StoriciCompressedSchema.somma_eaf5, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    19 -> TagXml(tagName = StoriciXMLSchema.EaF6, infoFrom = StoriciCompressedSchema.somma_eaf6, formatDouble = true, onlyPositiveValue = true, emptyTagIfNegativeValue = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    20 -> TagXml(tagName = StoriciXMLSchema.PotM, infoFrom = StoriciCompressedSchema.last_potf1, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore != "G" && podMeta.trattamento == "M")),
    21 -> TagXml(tagName = StoriciXMLSchema.PotF1, infoFrom = StoriciCompressedSchema.last_potf1, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    22 -> TagXml(tagName = StoriciXMLSchema.PotF2, infoFrom = StoriciCompressedSchema.last_potf2, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    23 -> TagXml(tagName = StoriciXMLSchema.PotF3, infoFrom = StoriciCompressedSchema.last_potf3, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G" || List("F","C").contains(podMeta.trattamento))),
    24 -> TagXml(tagName = StoriciXMLSchema.PotF4, infoFrom = StoriciCompressedSchema.last_potf4, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    25 -> TagXml(tagName = StoriciXMLSchema.PotF5, infoFrom = StoriciCompressedSchema.last_potf5, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    26 -> TagXml(tagName = StoriciXMLSchema.PotF6, infoFrom = StoriciCompressedSchema.last_potf6, formatDouble = true, presenceCondition = (podMeta) => (podMeta.tipoMisuratore == "G")),
    27 -> TagXml(tagName = StoriciXMLSchema.Consumo, onlyClosure = true),
    28 -> TagXml(tagName = StoriciXMLSchema.Consumo_StD, onlyOpening = true, presenceCondition = (podMeta) => (podMeta.podRiconfigurato && podMeta.nomeFlusso == "S2G" && podMeta.trattamento == "O")),
    29 -> TagXml(tagName = StoriciXMLSchema.EaF1_StD, infoFrom = StoriciCompressedSchema.somma_eaf1_riconf, formatDouble = true, presenceCondition = (podMeta) => (podMeta.podRiconfigurato && podMeta.nomeFlusso == "S2G" && podMeta.trattamento == "O")),
    30 -> TagXml(tagName = StoriciXMLSchema.EaF2_StD, infoFrom = StoriciCompressedSchema.somma_eaf2_riconf, formatDouble = true, presenceCondition = (podMeta) => (podMeta.podRiconfigurato && podMeta.nomeFlusso == "S2G" && podMeta.trattamento == "O")),
    31 -> TagXml(tagName = StoriciXMLSchema.EaF3_StD, infoFrom = StoriciCompressedSchema.somma_eaf3_riconf, formatDouble = true, presenceCondition = (podMeta) => (podMeta.podRiconfigurato && podMeta.nomeFlusso == "S2G" && podMeta.trattamento == "O")),
    32 -> TagXml(tagName = StoriciXMLSchema.Consumo_StD, onlyClosure = true, presenceCondition = (podMeta) => (podMeta.podRiconfigurato && podMeta.nomeFlusso == "S2G" && podMeta.trattamento == "O")),
    33 -> TagXml(tagName = StoriciXMLSchema.DatiPod, onlyClosure = true)
  )

}
