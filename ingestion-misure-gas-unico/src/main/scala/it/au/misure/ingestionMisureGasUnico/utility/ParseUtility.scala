package it.au.misure.ingestionMisureGasUnico.utility

import java.io.File

import it.au.misure.ingestionMisureGasUnico.model.GasMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RettificaXMLSchema
import org.apache.spark.sql.Row

object ParseUtility {
  def parseXmlMisura(gasMetadata: GasMetadata): List[Row] = {
    val localRootPath = PropertyUtility.getUnzipInputPath
    val xml = gasMetadata.loadXml
    val localFile = s"$localRootPath/${gasMetadata.originalRelativePath}"

    val codFlusso = (xml \ MisuraXMLSchema.cod_flusso).text
    val pivaUtente = (xml \ MisuraXMLSchema.IdentificativiFlusso \ MisuraXMLSchema.piva_utente).text
    val pivaDistr = (xml \ MisuraXMLSchema.IdentificativiFlusso \ MisuraXMLSchema.piva_distr).text

    (xml \ MisuraXMLSchema.DatiPdr).toList.flatMap(pdrNode => {
      val codPdr = (pdrNode \ MisuraXMLSchema.cod_pdr).text
      val meseComp = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.mese_comp)
      val dataPrest = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.data_prest)
      val codPratSII = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.CodPrat_SII)

      val trattamento = (pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.Trattamento).text
      val matrMis = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.matr_mis)
      val matrConv = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.matr_conv)
      val nCifreMis = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.n_cifre_mis)
      val nCifreConv = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.n_cifre_conv)
      val coeffCorr = (pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.coeff_corr).text
      val freqLet = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.freq_let)
      val accMis = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.acc_mis)
      val raccolta = (pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.Raccolta).text
      val esitoRaccolta = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.esito_raccolta)
      val causaMancRaccolta = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.causa_manc_raccolta)
      val modAltRacc = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.mod_alt_racc)
      val dirIndennizzo = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.dir_indennizzo)
      val prosFinInizio = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.pros_fin_inizio)
      val prosFinFine = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.pros_fin_fine)
      val volAnnuoSost = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.vol_annuo_sost)
      val classeGruppoMis = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.classe_gruppo_mis)
      val preConv = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.pre_conv)
      val gruppoMisInt = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiTecnPdr \ MisuraXMLSchema.gruppo_mis_int)

      if ((pdrNode \ MisuraXMLSchema.DatiLettura).isEmpty) {
        (pdrNode \ MisuraXMLSchema.LettureGiornaliere).toList.map(letturaNode => {
          val matrMisGiornaliere = FileUtility.extractNodeOrNull(letturaNode \ MisuraXMLSchema.matr_mis)
          val matrConvGiornaliere = FileUtility.extractNodeOrNull(letturaNode \ MisuraXMLSchema.matr_conv)
          val tipoLettura = (letturaNode \ MisuraXMLSchema.tipo_lettura).text
          val esitoVal: String = null // not in TGL
          val note: String = null // not in TGL
          val numTentativi: String = null // not in TGL
          val dataRacc: String = null // not in TGL
          val letTotPrel = FileUtility.extractNodeOrNull(letturaNode \ MisuraXMLSchema.let_tot_prel)
          val letTotConv = FileUtility.extractNodeOrNull(letturaNode \ MisuraXMLSchema.let_tot_conv)
          val dataMisEff: String = null // not in TGL
          val segnMisEff: String = null // not in TGL
          val segnConvEff: String = null // not in TGL
          val dataComp = (letturaNode \ MisuraXMLSchema.data_comp).text
          Row(
            codFlusso
            , pivaUtente
            , pivaDistr
            , codPdr
            , meseComp
            , dataPrest
            , codPratSII
            , trattamento
            , matrMis
            , matrConv
            , nCifreMis
            , nCifreConv
            , coeffCorr
            , freqLet
            , accMis
            , raccolta
            , esitoRaccolta
            , causaMancRaccolta
            , modAltRacc
            , dirIndennizzo
            , prosFinInizio
            , prosFinFine
            , volAnnuoSost
            , classeGruppoMis
            , preConv
            , gruppoMisInt
            , tipoLettura
            , esitoVal
            , note
            , numTentativi
            , dataRacc
            , letTotPrel
            , letTotConv
            , dataMisEff
            , segnMisEff
            , segnConvEff
            , matrMisGiornaliere
            , matrConvGiornaliere
            , dataComp
            , gasMetadata.getAmmissibilita(codPdr)
            , localFile
            , gasMetadata.anno
            , gasMetadata.annoRiferimento
            , gasMetadata.mese
            , gasMetadata.meseRiferimento
            , gasMetadata.giorno
            , new File(gasMetadata.originalRelativePath).getName
          )
        })
      } else {
        val matrMisGiornaliere: String = null // only in TGL
        val matrConvGiornaliere: String = null // only in TGL
        val tipoLettura = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.tipo_lettura)
        val esitoVal = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.esito_val)
        val note = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.Note)
        val numTentativi = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.num_tentativi)
        val dataRacc = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.data_racc)
        val letTotPrel = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.let_tot_prel)
        val letTotConv = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.let_tot_conv)
        val dataMisEff = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.data_mis_eff)
        val segnMisEff = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.segn_mis_eff)
        val segnConvEff = FileUtility.extractNodeOrNull(pdrNode \ MisuraXMLSchema.DatiLettura \ MisuraXMLSchema.segn_conv_eff)
        val dataComp: String = null // only in TGL
        List(Row(
          codFlusso
          , pivaUtente
          , pivaDistr
          , codPdr
          , meseComp
          , dataPrest
          , codPratSII
          , trattamento
          , matrMis
          , matrConv
          , nCifreMis
          , nCifreConv
          , coeffCorr
          , freqLet
          , accMis
          , raccolta
          , esitoRaccolta
          , causaMancRaccolta
          , modAltRacc
          , dirIndennizzo
          , prosFinInizio
          , prosFinFine
          , volAnnuoSost
          , classeGruppoMis
          , preConv
          , gruppoMisInt
          , tipoLettura
          , esitoVal
          , note
          , numTentativi
          , dataRacc
          , letTotPrel
          , letTotConv
          , dataMisEff
          , segnMisEff
          , segnConvEff
          , matrMisGiornaliere
          , matrConvGiornaliere
          , dataComp
          , gasMetadata.getAmmissibilita(codPdr)
          , localFile
          , gasMetadata.anno
          , gasMetadata.annoRiferimento
          , gasMetadata.mese
          , gasMetadata.meseRiferimento
          , gasMetadata.giorno
          , new File(gasMetadata.originalRelativePath).getName
        ))
      }
    })
  }

  def parseXmlRettifica(gasMetadata: GasMetadata): List[Row] = {
    val localRootPath = PropertyUtility.getUnzipInputPath
    val xml = gasMetadata.loadXml
    val localFile = s"$localRootPath/${gasMetadata.originalRelativePath}"

    val codFlusso = (xml \ RettificaXMLSchema.cod_flusso).text
    val pivaUtente = (xml \ RettificaXMLSchema.IdentificativiFlusso \ RettificaXMLSchema.piva_utente).text
    val pivaDistr = (xml \ RettificaXMLSchema.IdentificativiFlusso \ RettificaXMLSchema.piva_distr).text

    (xml \ RettificaXMLSchema.DatiPdr).toList.flatMap(pdrNode => {
      val codPdr = (pdrNode \ RettificaXMLSchema.cod_pdr).text
      val meseComp = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.mese_comp)
      val dataComp = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.data_comp)
      val tipoRettifica = (pdrNode \ RettificaXMLSchema.tipo_rettifica).text
      val motRetLett = (pdrNode \ RettificaXMLSchema.mot_ret_lett).text
      val causaOstativa = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.causa_ostativa)
      val dataPrest = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.data_prest)
      val codPratSII = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.CodPrat_SII)

      val trattamento = (pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.Trattamento).text
      val matrMis = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.matr_mis)
      val matrConv = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.matr_conv)
      val coeffCorr = (pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.coeff_corr).text
      val freqLet = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.freq_let)
      val volAnnuoRettificato = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiTecnPdrRett \ RettificaXMLSchema.vol_annuo_rettificato)

      if ((pdrNode \ RettificaXMLSchema.DatiLetturaRett).isEmpty) {

        if ((pdrNode \ RettificaXMLSchema.LettureGiornaliereRett).toList.nonEmpty) {

        (pdrNode \ RettificaXMLSchema.LettureGiornaliereRett).toList.map(letturaNode => {
          val matrMisGiornaliere = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.matr_mis)
          val matrConvGiornaliere = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.matr_conv)
          val dataRacc = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.data_racc)
          val letTotPrel = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.let_tot_prel)
          val letTotConv = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.let_tot_conv)
          val volRic = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.vol_ric)
          val iniPeriodo: String = null // not in RGL
          val finePeriodo: String = null // not in RGL
          val periodoRic = FileUtility.extractNodeOrNull(letturaNode \ RettificaXMLSchema.periodo_ric)
          Row(
            codFlusso
            , pivaUtente
            , pivaDistr
            , codPdr
            , meseComp
            , dataComp
            , tipoRettifica
            , motRetLett
            , causaOstativa
            , dataPrest
            , codPratSII
            , trattamento
            , matrMis
            , matrConv
            , coeffCorr
            , freqLet
            , volAnnuoRettificato
            , dataRacc
            , letTotPrel
            , letTotConv
            , volRic
            , iniPeriodo
            , finePeriodo
            , matrMisGiornaliere
            , matrConvGiornaliere
            , periodoRic
            , gasMetadata.getAmmissibilita(codPdr)
            , localFile
            , gasMetadata.anno
            , gasMetadata.annoRiferimento
            , gasMetadata.mese
            , gasMetadata.meseRiferimento
            , gasMetadata.giorno
            , new File(gasMetadata.originalRelativePath).getName
          )
        })
        }
        else {
          val matrMisGiornaliere: String = null
          val matrConvGiornaliere: String = null
          val dataRacc: String = null
          val letTotPrel: String = null
          val letTotConv: String = null
          val volRic: String = null
          val iniPeriodo: String = null // not in RGL
          val finePeriodo: String = null // not in RGL
          val periodoRic: String = null
          List(Row(
            codFlusso
            , pivaUtente
            , pivaDistr
            , codPdr
            , meseComp
            , dataComp
            , tipoRettifica
            , motRetLett
            , causaOstativa
            , dataPrest
            , codPratSII
            , trattamento
            , matrMis
            , matrConv
            , coeffCorr
            , freqLet
            , volAnnuoRettificato
            , dataRacc
            , letTotPrel
            , letTotConv
            , volRic
            , iniPeriodo
            , finePeriodo
            , matrMisGiornaliere
            , matrConvGiornaliere
            , periodoRic
            , gasMetadata.getAmmissibilita(codPdr)
            , localFile
            , gasMetadata.anno
            , gasMetadata.annoRiferimento
            , gasMetadata.mese
            , gasMetadata.meseRiferimento
            , gasMetadata.giorno
            , new File(gasMetadata.originalRelativePath).getName
          ))

        }

      } else { //DatiLetturaRett
        val matrMisGiornaliere: String = null // only in RGL
        val matrConvGiornaliere: String = null // only in RGL
        val dataRacc = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.data_racc)
        val letTotPrel = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.let_tot_prel)
        val letTotConv = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.let_tot_conv)
        val volRic = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.vol_ric)
        val iniPeriodo = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.ini_periodo)
        val finePeriodo = FileUtility.extractNodeOrNull(pdrNode \ RettificaXMLSchema.DatiLetturaRett \ RettificaXMLSchema.fine_periodo)
        val periodoRic: String = null // only in RGL
        List(Row(
          codFlusso
          , pivaUtente
          , pivaDistr
          , codPdr
          , meseComp
          , dataComp
          , tipoRettifica
          , motRetLett
          , causaOstativa
          , dataPrest
          , codPratSII
          , trattamento
          , matrMis
          , matrConv
          , coeffCorr
          , freqLet
          , volAnnuoRettificato
          , dataRacc
          , letTotPrel
          , letTotConv
          , volRic
          , iniPeriodo
          , finePeriodo
          , matrMisGiornaliere
          , matrConvGiornaliere
          , periodoRic
          , gasMetadata.getAmmissibilita(codPdr)
          , localFile
          , gasMetadata.anno
          , gasMetadata.annoRiferimento
          , gasMetadata.mese
          , gasMetadata.meseRiferimento
          , gasMetadata.giorno
          , new File(gasMetadata.originalRelativePath).getName
        ))
      }
    })
  }
}
