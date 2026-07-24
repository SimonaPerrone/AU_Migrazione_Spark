package it.au.misure.ingestionMisureGasUnico.validate

import java.time.LocalDateTime

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters
import junit.framework.TestCase
import org.junit.Assert

class TestCheckAmmissibiitaPDRRulesIGMG extends TestCase{

  val checkAmm = new CheckAmmissibilitaPDRRulesIGMG

  def testRuleRCUGAS1(): Unit ={
    val xmlMetaInRcu = GasXmlMetadata(
      xmlNode = null
      , file = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp = ""
      , progressivo = ""
      , tS = ""
    )
    val xmlMetaNotInRcu = GasXmlMetadata(
      xmlNode = null
      , file = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , pdrRcuExist = false
    )
    Assert.assertFalse(checkAmm.ruleRCUGAS1.condition(null, xmlMetaInRcu,None))
    Assert.assertTrue(checkAmm.ruleRCUGAS1.condition(null, xmlMetaNotInRcu,None))
  }

  def testRuleRCUGAS2(): Unit ={
    val pdrNode = <DatiPdR><data_misura>12/12/2020</data_misura></DatiPdR>

      val xmlMetaGood = GasXmlMetadata(
      xmlNode = null
      , file = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , pdrValidFrom = LocalDateTime.of(2020,12,12,0,0,0)
      , pdrValidTo = LocalDateTime.of(2020,12,12,0,0,0)
    )
    val xmlMetaBad = GasXmlMetadata(
      xmlNode = null
      , file = null
      , pivaDistributore = ""
      , pivaUtente = ""
      , anno = ""
      , annoRiferimento = ""
      , mese = ""
      , meseRiferimento = ""
      , giorno = ""
      , flusso = ""
      , timestamp = ""
      , progressivo = ""
      , tS = ""
      , pdrValidFrom = LocalDateTime.MAX
      , pdrValidTo = LocalDateTime.MAX
    )
    Assert.assertFalse(checkAmm.ruleRCUGAS2.condition(pdrNode, xmlMetaGood,None))
    Assert.assertTrue(checkAmm.ruleRCUGAS2.condition(pdrNode, xmlMetaBad,None))
  }

  def testRuleCauIntMisMatrMis(): Unit = {
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <matr_mis>1234aaabbb1234</matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <matr_mis></matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue( checkAmm.ruleCauIntMisMatrMis.condition(pdrNodeBadValued,null,None) )
    Assert.assertTrue( checkAmm.ruleCauIntMisMatrMis.condition(pdrNodeBadValued2,null,None) )
    Assert.assertFalse( checkAmm.ruleCauIntMisMatrMis.condition(pdrNodeGoodValued,null,None) )
  }

  def testRulePreIntPreConvGruppoMisInt(): Unit = { //ID 12
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int></gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <Pre-int>
        <pre_conv>NO</pre_conv>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisInt.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisInt.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvGruppoMisInt.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePreIntPreConvGruppoMisInt2(): Unit = { // ID 13
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int>
        <pre_conv></pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <Pre-int>

        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisInt2.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvGruppoMisInt2.condition(pdrNodeGoodValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisInt2.condition(pdrNodeBadValued2,null,None))
  }

  def testRulePreIntPreConvLetCorrettore(): Unit = { //id 14
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <let_correttore>000000000000133</let_correttore>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <let_correttore></let_correttore>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <pre_conv>SI</pre_conv>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvLetCorrettore.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvLetCorrettore.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvLetCorrettore.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePreIntCoeffCorr(): Unit = { //id 15
    val paramX = 10

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("X"-> paramX.toString)
    )

    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <coeff_corr>{(paramX-1).toString}</coeff_corr>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int>
        <coeff_corr>{(paramX+1).toString}</coeff_corr>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntCoeffCorr.condition(pdrNodeBadValued,null,Option(ruleParameters)))
    Assert.assertFalse(checkAmm.rulePreIntCoeffCorr.condition(pdrNodeGoodValued,null,Option(ruleParameters)))

  }

  def testRulePostIntCoeffCorr(): Unit = { //id 16
    val paramX = 10

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("X"-> paramX.toString)
    )

    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <coeff_corr>{(paramX-1).toString}</coeff_corr>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <coeff_corr>{(paramX+1).toString}</coeff_corr>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostIntCoeffCorr.condition(pdrNodeBadValued,null,Option(ruleParameters)))
    Assert.assertFalse(checkAmm.rulePostIntCoeffCorr.condition(pdrNodeGoodValued,null,Option(ruleParameters)))

  }

  def testRulePreIntPreConvCauIntMatrConv(): Unit = { //id 17
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_cor>valued</cau_int_cor>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <matr_conv>valued</matr_conv>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_cor>valued</cau_int_cor>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <matr_conv></matr_conv>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_cor>valued</cau_int_cor>
      <Pre-int>
        <pre_conv>SI</pre_conv>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvCauIntMatrConv.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvCauIntMatrConv.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvCauIntMatrConv.condition(pdrNodeGoodValued,null,None))
  }

  def testRulePostInitCauIntMisMatrMis(): Unit = { // id 18
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis></matr_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis>aaaaaaaaa</matr_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisMatrMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisMatrMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisMatrMis2(): Unit = { // id 19
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis>valued</matr_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis></matr_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisMatrMis2.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisMatrMis2.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisTipoMis(): Unit = { // id 20
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisTipoMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisTipoMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitTipoMisCauInitMisTelegestione(): Unit = { // id 21
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
        <telegestione>SI</telegestione>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
        <telegestione></telegestione>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitTipoMisCauInitMisTelegestione.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitTipoMisCauInitMisTelegestione.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvGruppoMis(): Unit = { // id 22
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv></pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvGruppoMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvGruppoMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvGruppoMisInt(): Unit = { // id 23
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int></gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvGruppoMisInt.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvGruppoMisInt.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvLetCorrettore(): Unit = { // id 24
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <let_correttore>000000000000001</let_correttore>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <let_correttore></let_correttore>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvLetCorrettore.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvLetCorrettore.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvMatrConv(): Unit = { // id 25
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <matr_conv>NO</matr_conv>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <matr_conv></matr_conv>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvMatrConv.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvMatrConv.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisClasseGruppoMis(): Unit = { // id 26
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis></classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G4</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisClasseGruppoMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisClasseGruppoMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitClasseGruppoMis(): Unit = { // id 27
    val pdrNodeGoodValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G1,6</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G2,55</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitClasseGruppoMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitClasseGruppoMis.condition(pdrNodeGoodValued,null,None))

  }


  def testRulePostInitCauIntMisAccMis(): Unit = { // id 28
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <acc_mis></acc_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <acc_mis>3</acc_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisAccMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisAccMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRuleCoeffCorr(): Unit = {

    val paramXMinimo  = 0
    val paramXMassimo = 99999.0

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("XMinimo"-> paramXMinimo.toString, "XMassimo" -> paramXMassimo.toString)
    )

    val pdrNodeGoodValued1 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeGoodValued2 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo+1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo+1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeGoodValued3 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeGoodValued4 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued1 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued2 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued3 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued4 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued5 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued6 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued7 = <DatiPdr>
      <Pre-int>
        <coeff_corr></coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued8 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr></coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued9 = <DatiPdr>
      <Pre-int>
      </Pre-int>
      <Post-int>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued10 = <DatiPdr>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued11 = <DatiPdr>
      <Pre-int>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued1, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued2, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued3, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued4, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued5, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued6, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued7, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued8, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued9, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued10, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeBadValued11, null, None))

    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeGoodValued1, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeGoodValued2, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeGoodValued3, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMG.condition(pdrNodeGoodValued4, null, None))

  }

  def testRulePreConvCauIntCorIGMG(): Unit = { // id 32
    //test KO on Pre-Int section
    val pdrNodeBadPre =
      <DatiPdR>
        <cau_int_cor>ERR_LETTURA</cau_int_cor>
          <Pre-int>
            <pre_conv>NO</pre_conv>
          </Pre-int>
          <Post-int>
            <pre_conv>SI</pre_conv>
          </Post-int>
    </DatiPdR>


    //test KO on Post-int section
    val pdrNodeBadPost = <DatiPdR>
      <cau_int_cor>ERR_LETTURA</cau_int_cor>
      <Pre-int>
        <pre_conv>SI</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
      </Post-int>
    </DatiPdR>

    // error on both sections
    val pdrNodeBadBoth = <DatiPdR>
      <cau_int_cor>ERR_DOPPIO</cau_int_cor>
      <Pre-int>
        <pre_conv>NO</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
      </Post-int>
    </DatiPdR>

    //test case ok, pre_conv NO without cau
    val pdrNodeNoCause = <DatiPdR>
      <cau_int_cor></cau_int_cor>
      <Pre-int>
        <pre_conv>NO</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
      </Post-int>
    </DatiPdR>

    //test case ok
    val pdrNodeAllOk = <DatiPdR>
      <cau_int_cor>ERR_LETTURA</cau_int_cor>
      <Pre-int>
        <pre_conv>SI</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
      </Post-int>
    </DatiPdR>



    Assert.assertTrue(checkAmm.rulePreConvCauIntCorIGMG.condition(pdrNodeBadPre, null, None))
    Assert.assertTrue(checkAmm.rulePreConvCauIntCorIGMG.condition(pdrNodeBadPost, null, None))
    Assert.assertTrue(checkAmm.rulePreConvCauIntCorIGMG.condition(pdrNodeBadBoth, null, None))
    Assert.assertFalse(checkAmm.rulePreConvCauIntCorIGMG.condition(pdrNodeNoCause, null, None))
    Assert.assertFalse(checkAmm.rulePreConvCauIntCorIGMG.condition(pdrNodeAllOk, null, None))

  }
}
