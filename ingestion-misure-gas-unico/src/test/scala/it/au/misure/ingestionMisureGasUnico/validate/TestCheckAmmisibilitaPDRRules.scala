package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import junit.framework.TestCase
import org.junit.Assert

class TestCheckAmmisibilitaPDRRules extends TestCase {
  val checkAmm = new CheckAmmissibilitaPDRRules

  def testRuleMeseComp(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = TGL,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaGoodValued2 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = RMV,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val pdrNodeGoodValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <mese_comp>11/2019</mese_comp>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleMeseComp.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleMeseComp.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleMeseComp.condition(pdrNodeGoodValued, xmlMetaGoodValued2, None))
    Assert.assertTrue(checkAmm.ruleMeseComp.condition(pdrNodeBadValued, xmlMetaGoodValued2, None))

  }

  def testRuleDataCompLettureGiornaliere(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = TGL,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaFlusso2 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = RML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val pdrNodeGoodValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2019</data_comp>
      </LettureGiornaliere>
    </DatiPdr>

    val pdrNodeBadValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>

    val pdrNodeGoodValued2 = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdr>
        <Trattamento>F</Trattamento>
      </DatiTecnPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleDataCompLettureGiornaliere.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleDataCompLettureGiornaliere.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleDataCompLettureGiornaliere.condition(pdrNodeGoodValued2, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleDataCompLettureGiornaliere.condition(pdrNodeBadValued, xmlMetaFlusso2, None))

  }

  def testRuleDataRaccDataComp(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = TGL,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val xmlMetaGoodValued2 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2019</data_comp>
      </LettureGiornaliere>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>

    val pdrNodeGoodValued2 = <DatiPdr>
      <DatiLettura>
        <data_racc>09/12/2019</data_racc>
      </DatiLettura>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <data_comp></data_comp>
      <DatiLettura>
        <data_racc>09/11/2019</data_racc>
      </DatiLettura>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleDataRaccDataComp.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleDataRaccDataComp.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleDataRaccDataComp.condition(pdrNodeGoodValued2, xmlMetaGoodValued2, None))
    Assert.assertTrue(checkAmm.ruleDataRaccDataComp.condition(pdrNodeBadValued2, xmlMetaGoodValued2, None))

  }

  def testRuleDataRaccLettureGiornaliereRett(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = RML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaFlusso1 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val pdrNodeGoodValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
      <LettureGiornaliereRett>
        <data_racc>05/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>15/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>12/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>29/12/2019</data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>

    val pdrNodeBadValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
      <LettureGiornaliereRett>
        <data_racc>05/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>15/12/2020</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>12/12/2020</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>29/12/2020</data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>

    val pdrNodeGoodValued2 = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdrRett>
        <Trattamento>F</Trattamento>
      </DatiTecnPdrRett>
      <LettureGiornaliereRett>
        <data_racc>05/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>15/12/2020</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>12/12/2020</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>29/12/2020</data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleDataRaccLettureGiornaliereRett.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleDataRaccLettureGiornaliereRett.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleDataRaccLettureGiornaliereRett.condition(pdrNodeGoodValued2, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleDataRaccLettureGiornaliereRett.condition(pdrNodeBadValued, xmlMetaFlusso1, None))

  }

  def testRuleDataPrest(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "12",
      giorno = "",
      flusso = SWG1,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <data_prest>18/12/2019</data_prest>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <data_prest>31/11/2019</data_prest>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleDataPrest.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleDataPrest.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
  }

  def testRuleUDDDataPrest(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "09",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val xmlNode = <DatiPdr>
      <data_prest>10/10/2019</data_prest>
      <DatiTecnPdr>
        <Raccolta>S</Raccolta>
      </DatiTecnPdr>
      <DatiLettura>
      </DatiLettura>
    </DatiPdr>

    val xmlMetaBadValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "10",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val xmlMetaDontCare = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "2019",
      mese = "",
      meseRiferimento = "10",
      giorno = "",
      flusso = A01,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    Assert.assertFalse(checkAmm.ruleUDDDataPrest.condition(xmlNode, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleUDDDataPrest.condition(xmlNode, xmlMetaBadValued, None))
    Assert.assertFalse(checkAmm.ruleUDDDataPrest.condition(xmlNode, xmlMetaDontCare, None))

  }

  def testRuleTrattamento1(): Unit = {

    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = TGL,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val xmlMetaGoodValued2 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = RGL,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <Trattamento>S</Trattamento>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeGoodValued2 = <DatiPdr>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdrRett>
        <Trattamento>S</Trattamento>
      </DatiTecnPdrRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleTrattamento1.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTrattamento1.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleTrattamento1.condition(pdrNodeGoodValued2, xmlMetaGoodValued2, None))
    Assert.assertTrue(checkAmm.ruleTrattamento1.condition(pdrNodeBadValued2, xmlMetaGoodValued2, None))
  }

  def testRuleTrattamento2(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <Trattamento>M</Trattamento>
        <Raccolta>P</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <Trattamento>S</Trattamento>
        <Raccolta>P</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>

    val xmlMetaGoodValued2 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = RML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued2 = <DatiPdr>
      <DatiTecnPdrRett>
        <Trattamento>M</Trattamento>
        <tipo_rettifica>P</tipo_rettifica>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdrRett>
        <Trattamento>S</Trattamento>
        <tipo_rettifica>P</tipo_rettifica>
      </DatiTecnPdrRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleTrattamento2.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTrattamento2.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleTrattamento2.condition(pdrNodeGoodValued2, xmlMetaGoodValued2, None))
    Assert.assertTrue(checkAmm.ruleTrattamento2.condition(pdrNodeBadValued2, xmlMetaGoodValued2, None))
  }

  def testRuleTrattamentoMesecomp(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = TML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <mese_comp></mese_comp>
      <DatiTecnPdr>
        <Trattamento>G</Trattamento>
      </DatiTecnPdr>
    </DatiPdr>

    val xmlMetaGoodValued0 = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = RML,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued0 = <DatiPdr>
      <mese_comp>12/2019</mese_comp>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued0 = <DatiPdr>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued02 = <DatiPdr>
      <mese_comp></mese_comp>
      <DatiTecnPdrRett>
        <Trattamento>G</Trattamento>
      </DatiTecnPdrRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeGoodValued0, xmlMetaGoodValued0, None))
    Assert.assertTrue(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeBadValued0, xmlMetaGoodValued0, None))
    Assert.assertTrue(checkAmm.ruleTrattamentoMesecomp.condition(pdrNodeBadValued02, xmlMetaGoodValued0, None))
  }

  def testRuleCodFlusso1DataRacc(): Unit = {

    Set(TML, TMV, TAL, TAS, TAV, SWG1, FUI, FDD).foreach(cod_flusso => {
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={cod_flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = cod_flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <mese_comp>12/2019</mese_comp>
        <DatiLettura>
          <data_racc>09/12/2019</data_racc>
        </DatiLettura>
      </DatiPdr>
      Assert.assertFalse(checkAmm.ruleCodFlusso1DataRacc.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    })

    Set(TML, TMV, TAL, TAS, TAV, SWG1, FUI, FDD).foreach(cod_flusso => {
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={cod_flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = cod_flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeBadValued = <DatiPdr>
        <mese_comp>12/2019</mese_comp>
        <DatiLettura>
          <data_racc></data_racc>
        </DatiLettura>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mese_comp>12/2019</mese_comp>
        <DatiLettura>
          <data_racc></data_racc>
        </DatiLettura>
      </DatiPdr>

      Assert.assertTrue(checkAmm.ruleCodFlusso1DataRacc.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleCodFlusso1DataRacc.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    })

  }

  def testRuleCodFlusso1DataComp(): Unit = {

    val codiceFlusso = TGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <LettureGiornaliere>
        <data_comp>01/12/2019</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <LettureGiornaliere>
        <data_comp></data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <LettureGiornaliere>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>10/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>11/12/2020</data_comp>
      </LettureGiornaliere>
      <LettureGiornaliere>
        <data_comp>31/12/2020</data_comp>
      </LettureGiornaliere>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleCodFlusso1DataComp.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso1DataComp.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso1DataComp.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))

  }

  def testRuleCodFlusso2DataRaccRGL(): Unit = {

    val codiceFlusso = RGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <LettureGiornaliereRett>
        <data_racc>09/12/2019</data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <LettureGiornaliereRett>
        <data_racc></data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <LettureGiornaliereRett></LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>09/12/2019</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>09/12/2018</data_racc>
      </LettureGiornaliereRett>
      <LettureGiornaliereRett>
        <data_racc>14/12/2019</data_racc>
      </LettureGiornaliereRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleCodFlusso2DataRaccRGL.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso2DataRaccRGL.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso2DataRaccRGL.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleCodFlusso2DataRaccRML(): Unit = {

    val codiceFlusso = RML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiLetturaRett>
        <data_racc>09/12/2019</data_racc>
      </DatiLetturaRett>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiLetturaRett>
        <data_racc></data_racc>
      </DatiLetturaRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiLetturaRett></DatiLetturaRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleCodFlusso2DataRaccRML.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso2DataRaccRML.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlusso2DataRaccRML.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleRaccoltaCodPratSII(): Unit = {

    val codiceFlusso = TML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>X</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeGoodValued1 = <DatiPdr>
      <CodPrat_SII>valued</CodPrat_SII>
      <DatiTecnPdr>
        <Raccolta>S</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <CodPrat_SII></CodPrat_SII>
      <DatiTecnPdr>
        <Raccolta>V</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>S</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleRaccoltaCodPratSII.condition(pdrNodeGoodValued1, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleRaccoltaCodPratSII.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleRaccoltaCodPratSII.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleRaccoltaCodPratSII.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleRaccoltaDataPrest(): Unit = {

    val codiceFlusso = TGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <data_prest>18/12/2019</data_prest>
      <DatiTecnPdr>
        <Raccolta>S</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <data_prest></data_prest>
      <DatiTecnPdr>
        <Raccolta>T</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>V</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleRaccoltaDataPrest.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleRaccoltaDataPrest.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleRaccoltaDataPrest.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleTipoRettificaDataPrest(): Unit = {

    val codiceFlusso = RML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <data_prest>18/12/2019</data_prest>
      <tipo_rettifica>S</tipo_rettifica>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <data_prest></data_prest>
      <tipo_rettifica>T</tipo_rettifica>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <tipo_rettifica>V</tipo_rettifica>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleTipoRettificaDataPrest.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTipoRettificaDataPrest.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleTipoRettificaDataPrest.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleCodFlussoRaccolta(): Unit = {

    val codiceFlusso = TMV
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>V</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>S</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeBadValued, xmlMetaGoodValued, None))

    val flussiRaccoltaS = Set(SWG1, FUI, FDD)
    val flussiRaccoltaAC = Set(TAL, TAS, TAV)
    val flussiRaccoltaT = Set(D01, D02, R01, A40, S40, R40, A01, A02, S02, V01, M01, V02, SM1, SM2, AD2, AD3)

    flussiRaccoltaS.foreach(flusso => {
      val raccoltaGoodValued = "S"
      val raccoltaBadValued = "A"
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaGoodValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaBadValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeBadValued, xmlMetaGoodValued, None))

    })

    flussiRaccoltaAC.foreach(flusso => {
      val raccoltaGoodValued = "AC"
      val raccoltaBadValued = "A"
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaGoodValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaBadValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeBadValued, xmlMetaGoodValued, None))

    })

    flussiRaccoltaT.foreach(flusso => {
      val raccoltaGoodValued = "T"
      val raccoltaBadValued = "A"
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaGoodValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <DatiTecnPdr>
          <Raccolta>{raccoltaBadValued}</Raccolta>
        </DatiTecnPdr>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleCodFlussoRaccolta.condition(pdrNodeBadValued, xmlMetaGoodValued, None))

    })
  }

  def testRuleCodFlussoTipoRettifica(): Unit = {

    val codiceFlussoRMV = RMV
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlussoRMV}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlussoRMV,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValuedRMV = <DatiPdr>
      <tipo_rettifica>V</tipo_rettifica>
    </DatiPdr>
    val pdrNodeBadValuedRMV = <DatiPdr>
      <tipo_rettifica>S</tipo_rettifica>
    </DatiPdr>

    val xmlMetaGoodValuedRSL = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso="RSL"></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "RSL",
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValuedRSL = <DatiPdr>
      <tipo_rettifica>S</tipo_rettifica>
    </DatiPdr>
    val pdrNodeBadValuedRSL = <DatiPdr>
      <tipo_rettifica>V</tipo_rettifica>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeGoodValuedRMV, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeBadValuedRMV, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeGoodValuedRSL, xmlMetaGoodValuedRSL, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeBadValuedRSL, xmlMetaGoodValuedRSL, None))


    val flussiRaccoltaT = Set(D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R)

    flussiRaccoltaT.foreach(flusso => {
      val tipoRettificaGoodValued = "T"
      val tipoRettificaBadValued = "A"
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={flusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = flusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <tipo_rettifica>{tipoRettificaGoodValued}</tipo_rettifica>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <tipo_rettifica>{tipoRettificaBadValued}</tipo_rettifica>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleCodFlussoTipoRettifica.condition(pdrNodeBadValued, xmlMetaGoodValued, None))

    })
  }

  def testRuleCodFlussoEsitoRaccolta(): Unit = {

    val codiceFlusso = TML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <esito_raccolta>Valued</esito_raccolta>
        <Raccolta>P</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <esito_raccolta></esito_raccolta>
        <Raccolta>P</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdr>
        <Raccolta>P</Raccolta>
      </DatiTecnPdr>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleCodFlussoEsitoRaccolta.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoEsitoRaccolta.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoEsitoRaccolta.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))

  }

  def testRuleCodFlussoFreqLetFlusso1(): Unit = {

    val codiceFlusso = TML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <freq_let>1</freq_let>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <freq_let>11</freq_let>
      </DatiTecnPdr>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdr></DatiTecnPdr>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleCodFlussoFreqLetFlusso1.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoFreqLetFlusso1.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoFreqLetFlusso1.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))

  }

  def testRuleCodFlussoFreqLetFlusso2(): Unit = {

    val codiceFlusso = RML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdrRett>
        <freq_let>1</freq_let>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdrRett>
        <freq_let>11</freq_let>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdrRett></DatiTecnPdrRett>
    </DatiPdr>


    Assert.assertFalse(checkAmm.ruleCodFlussoFreqLetFlusso2.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoFreqLetFlusso2.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoFreqLetFlusso2.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleMotRetLetLetTotPrel(): Unit = {

    val flussoWhiteList = Set(RML, RMV, RSL, D01R, D02R, R01R, A40R, S40R, R40R, A01R, A02R, S02R, V01R, M01R, V02R, SM1R, SM2R, AD2R, AD3R)
    val morRetLettWhiteList = Set("1", "2", "4", "5")

    flussoWhiteList.foreach(codiceFlusso => {
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = codiceFlusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      morRetLettWhiteList.foreach(mot_ret_lett => {
        val pdrNodeGoodValued = <DatiPdr>
          <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
          <DatiLetturaRett>
            <let_tot_prel>valued</let_tot_prel>
          </DatiLetturaRett>
        </DatiPdr>
        val pdrNodeBadValued = <DatiPdr>
          <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
          <DatiLetturaRett>
            <let_tot_prel></let_tot_prel>
          </DatiLetturaRett>
        </DatiPdr>
        val pdrNodeBadValued2 = <DatiPdr>
          <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
          <DatiLetturaRett>
          </DatiLetturaRett>
        </DatiPdr>

        Assert.assertFalse(checkAmm.ruleMotRetLetLetTotPrel.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleMotRetLetLetTotPrel.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleMotRetLetLetTotPrel.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
      })
    })
  }

  def testRuleMotRetLetLetTotPrelRGL(): Unit = {

    val morRetLettWhiteList = Set("1", "2", "4", "5")

    val codiceFlusso = RGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    morRetLettWhiteList.foreach(mot_ret_lett => {
      val pdrNodeGoodValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <let_tot_prel>valued</let_tot_prel>
        </LettureGiornaliereRett>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <let_tot_prel></let_tot_prel>
        </LettureGiornaliereRett>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
        </LettureGiornaliereRett>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleMotRetLetLetTotPrelRGL.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleMotRetLetLetTotPrelRGL.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleMotRetLetLetTotPrelRGL.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    })
  }

  def testRulePreConvGruppoMisInt(): Unit = {

    val codFlussiList = Set(TMV, SWG1, FUI, FDD, A01, A02, A40, S02, S40, AD2, AD3)
    codFlussiList.foreach(codiceFlusso => {
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = codiceFlusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )
      val pdrNodeGoodValued = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>NO</pre_conv>
          <gruppo_mis_int>valued</gruppo_mis_int>
        </DatiTecnPdr>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>NO</pre_conv>
          <gruppo_mis_int></gruppo_mis_int>
        </DatiTecnPdr>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>NO</pre_conv>
        </DatiTecnPdr>
      </DatiPdr>

      Assert.assertFalse(checkAmm.rulePreConvGruppoMisInt.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.rulePreConvGruppoMisInt.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.rulePreConvGruppoMisInt.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    })
  }

  def testRulePreConvLetTotConv(): Unit = {

    val flussoWhiteList = Set(TML, TAL, TAS, TAV, TMV, SWG1, FUI, FDD, D01, R01, A40, S40, R40, A01, A02, S02, V01, M01, V02, SM1, SM2, AD2, AD3)


    flussoWhiteList.foreach(codiceFlusso => {
      val xmlMetaGoodValued = GasXmlMetadata(
        xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
        file = null,
        pivaDistributore = "",
        pivaUtente = "",
        anno = "",
        annoRiferimento = "",
        mese = "",
        meseRiferimento = "",
        giorno = "",
        flusso = codiceFlusso,
        timestamp = "",
        progressivo = "",
        tS = ""
      )

      val pdrNodeGoodValued = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>SI</pre_conv>
        </DatiTecnPdr>
        <DatiLettura>
          <let_tot_conv>valued</let_tot_conv>
        </DatiLettura>
      </DatiPdr>
      val pdrNodeBadValued = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>SI</pre_conv>
        </DatiTecnPdr>
        <DatiLettura>
          <let_tot_conv></let_tot_conv>
        </DatiLettura>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <DatiTecnPdr>
          <pre_conv>SI</pre_conv>
        </DatiTecnPdr>
        <DatiLettura></DatiLettura>
      </DatiPdr>

      Assert.assertFalse(checkAmm.rulePreConvLetTotConv.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.rulePreConvLetTotConv.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.rulePreConvLetTotConv.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))

    })
  }

  def testRulePresConvLetTotConvTGL(): Unit = {
    val codiceFlusso = TGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <pre_conv>SI</pre_conv>
      </DatiTecnPdr>
      <LettureGiornaliere>
        <let_tot_conv>valued</let_tot_conv>
      </LettureGiornaliere>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <pre_conv>SI</pre_conv>
      </DatiTecnPdr>
      <LettureGiornaliere>
        <let_tot_conv></let_tot_conv>
      </LettureGiornaliere>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdr>
        <pre_conv>SI</pre_conv>
      </DatiTecnPdr>
      <LettureGiornaliere></LettureGiornaliere>
    </DatiPdr>

    Assert.assertFalse(checkAmm.rulePresConvLetTotConvTGL.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.rulePresConvLetTotConvTGL.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.rulePresConvLetTotConvTGL.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))

  }

  def testRuleCodFlussoVolAnnuoRettificato(): Unit = {
    val codiceFlusso = RMV
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdrRett>
        <vol_annuo_rettificato>Valued</vol_annuo_rettificato>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdrRett>
        <vol_annuo_rettificato></vol_annuo_rettificato>
      </DatiTecnPdrRett>
    </DatiPdr>
    val pdrNodeBadValued2 = <DatiPdr>
      <DatiTecnPdrRett></DatiTecnPdrRett>
    </DatiPdr>

    Assert.assertFalse(checkAmm.ruleCodFlussoVolAnnuoRettificato.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoVolAnnuoRettificato.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertTrue(checkAmm.ruleCodFlussoVolAnnuoRettificato.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
  }

  def testRuleCodFlussoMotRetLetRML(): Unit = {
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso="RML"></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "RML",
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    List("3", "6").foreach(mot_ret_lett => {
      val pdrNodeBadValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett></DatiLetturaRett>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett>
          <data_racc></data_racc>
        </DatiLetturaRett>
      </DatiPdr>
      val pdrNodeGoodValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett>
          <data_racc>15/06/2020</data_racc>
        </DatiLetturaRett>
      </DatiPdr>

      if (mot_ret_lett.equals("3") || mot_ret_lett.equals("6")) {
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
      } else {
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRML.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
      }
    })
  }

  def testRuleCodFlussoMotRetLetRGL(): Unit = {

    val codiceFlusso = RGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    List("3", "6").foreach(mot_ret_lett => {
      val pdrNodeBadValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett></LettureGiornaliereRett>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <data_racc></data_racc>
        </LettureGiornaliereRett>
      </DatiPdr>
      val pdrNodeGoodValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <data_racc>15/06/2020</data_racc>
        </LettureGiornaliereRett>
      </DatiPdr>

      if (mot_ret_lett.equals("3") || mot_ret_lett.equals("6")) {
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
        Assert.assertTrue(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
      } else {
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
        Assert.assertFalse(checkAmm.ruleCodFlussoMotRetLetRGL.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
      }
    })
  }

  def testRule1MotRetLet(): Unit = {

    val codiceFlusso = RML
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    List("4", "5").foreach(mot_ret_lett => {
      val pdrNodeGoodValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett>
          <vol_ric>valued</vol_ric>
          <ini_periodo>valued</ini_periodo>
          <fine_periodo>valued</fine_periodo>
        </DatiLetturaRett>
      </DatiPdr>

      val pdrNodeBadValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett>
          <vol_ric>valued</vol_ric>
          <ini_periodo></ini_periodo>
          <fine_periodo>valued</fine_periodo>
        </DatiLetturaRett>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <DatiLetturaRett>
        </DatiLetturaRett>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleRMLMotRetLet.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleRMLMotRetLet.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleRMLMotRetLet.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    })
  }

  def testRule2MotRetLet(): Unit = {

    val codiceFlusso = RGL
    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={codiceFlusso}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = codiceFlusso,
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    List("4", "5").foreach(mot_ret_lett => {
      val pdrNodeGoodValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <vol_ric>valued</vol_ric>
          <periodo_ric>valued</periodo_ric>
        </LettureGiornaliereRett>
      </DatiPdr>

      val pdrNodeBadValued = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
          <vol_ric>valued</vol_ric>
          <periodo_ric></periodo_ric>
        </LettureGiornaliereRett>
      </DatiPdr>
      val pdrNodeBadValued2 = <DatiPdr>
        <mot_ret_lett>{mot_ret_lett}</mot_ret_lett>
        <LettureGiornaliereRett>
        </LettureGiornaliereRett>
      </DatiPdr>

      Assert.assertFalse(checkAmm.ruleRGLMotRetLet.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleRGLMotRetLet.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
      Assert.assertTrue(checkAmm.ruleRGLMotRetLet.condition(pdrNodeBadValued2, xmlMetaGoodValued, None))
    })
  }

  def testRuleClasseGruppoMisFlusso1(): Unit = {

    val pdrNodeGoodValued = <DatiPdr>
      <DatiTecnPdr>
        <classe_gruppo_mis>G1,6</classe_gruppo_mis>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeBadValued = <DatiPdr>
      <DatiTecnPdr>
        <classe_gruppo_mis>G5</classe_gruppo_mis>
      </DatiTecnPdr>
    </DatiPdr>

    val xmlMetaGoodValued = GasXmlMetadata(
      xmlNode = null,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = "TMV",
      timestamp = "",
      progressivo = "",
      tS = ""
    )
    Assert.assertTrue(checkAmm.ruleClasseGruppoMisFlusso1.condition(pdrNodeBadValued, xmlMetaGoodValued, None))
    Assert.assertFalse(checkAmm.ruleClasseGruppoMisFlusso1.condition(pdrNodeGoodValued, xmlMetaGoodValued, None))

  }

  def testRuleCoeffCorr(): Unit = {

    val paramXMinimo  = 0
    val paramXMassimo = 99999.0

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("XMinimo"-> paramXMinimo.toString, "XMassimo" -> paramXMassimo.toString)
    )

    val pdrNodeGoodValued1Flusso1 = <DatiPdr>
      <DatiTecnPdr>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeGoodValued1Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </DatiTecnPdrRett>
    </DatiPdr>

    val pdrNodeGoodValued2Flusso1 = <DatiPdr>
      <DatiTecnPdr>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeGoodValued2Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </DatiTecnPdrRett>
    </DatiPdr>

    val pdrNodeBadValued1Flusso1 = <DatiPdr>
      <DatiTecnPdr>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeBadValued1Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </DatiTecnPdrRett>
    </DatiPdr>

    val pdrNodeBadValued2Flusso1 = <DatiPdr>
      <DatiTecnPdr>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeBadValued2Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </DatiTecnPdrRett>
    </DatiPdr>

    val pdrNodeBadValued3Flusso1 = <DatiPdr>
      <DatiTecnPdr>
        <coeff_corr></coeff_corr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeBadValued3Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
        <coeff_corr></coeff_corr>
      </DatiTecnPdrRett>
    </DatiPdr>

    val pdrNodeBadValued4Flusso1 = <DatiPdr>
      <DatiTecnPdr>
      </DatiTecnPdr>
    </DatiPdr>

    val pdrNodeBadValued4Flusso2 = <DatiPdr>
      <DatiTecnPdrRett>
      </DatiTecnPdrRett>
    </DatiPdr>


    val xmlMetaGoodValuedFlusso1 = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={AD2}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = AD2,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    val xmlMetaGoodValuedFlusso2 = GasXmlMetadata(
      xmlNode = <FlussoMisure cod_flusso={AD2R}></FlussoMisure>,
      file = null,
      pivaDistributore = "",
      pivaUtente = "",
      anno = "",
      annoRiferimento = "",
      mese = "",
      meseRiferimento = "",
      giorno = "",
      flusso = AD2R,
      timestamp = "",
      progressivo = "",
      tS = ""
    )

    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued1Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued1Flusso2, xmlMetaGoodValuedFlusso2, None))

    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued2Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued2Flusso2, xmlMetaGoodValuedFlusso2, None))

    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued3Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued3Flusso2, xmlMetaGoodValuedFlusso2, None))

    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued4Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorr.condition(pdrNodeBadValued4Flusso2, xmlMetaGoodValuedFlusso2, None))

    Assert.assertFalse(checkAmm.ruleCoeffCorr.condition(pdrNodeGoodValued1Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorr.condition(pdrNodeGoodValued1Flusso2, xmlMetaGoodValuedFlusso2, None))

    Assert.assertFalse(checkAmm.ruleCoeffCorr.condition(pdrNodeGoodValued2Flusso1, xmlMetaGoodValuedFlusso1, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorr.condition(pdrNodeGoodValued2Flusso2, xmlMetaGoodValuedFlusso2, None))

  }
}
