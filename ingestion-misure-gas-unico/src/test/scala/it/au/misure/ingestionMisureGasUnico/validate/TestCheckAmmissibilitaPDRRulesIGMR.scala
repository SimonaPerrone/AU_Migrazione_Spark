package it.au.misure.ingestionMisureGasUnico.validate

import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.validate.RuleParameters
import junit.framework.TestCase
import org.junit.Assert

import java.time.LocalDateTime

class TestCheckAmmissibilitaPDRRulesIGMR extends TestCase {

  val checkAmm = new CheckAmmissibilitaPDRRulesIGMR

  def testRuleRCUGAS1(): Unit ={ //18
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
    Assert.assertFalse(checkAmm.ruleRCUGAS1IGMR.condition(null, xmlMetaInRcu,None))
    Assert.assertTrue(checkAmm.ruleRCUGAS1IGMR.condition(null, xmlMetaNotInRcu,None))
  }

  def testRuleRCUGAS2(): Unit ={//19
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
    Assert.assertFalse(checkAmm.ruleRCUGAS2IGMR.condition(pdrNode, xmlMetaGood,None))
    Assert.assertTrue(checkAmm.ruleRCUGAS2IGMR.condition(pdrNode, xmlMetaBad,None))
  }

  def testRuleIgmgIgmrMatch(): Unit = { //20
    val pdrNode = <DatiPdR>
      <data_misura>12/12/2020</data_misura>
    </DatiPdR>

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
      , pdrValidFrom = LocalDateTime.of(2020, 12, 12, 0, 0, 0)
      , pdrValidTo = LocalDateTime.of(2020, 12, 12, 0, 0, 0)
      , igmgMatch = "RELATIVO_IGMG_PRESENTE"
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
      , igmgMatch = ""
    )
    Assert.assertFalse(checkAmm.ruleIgmgIgmrMatch.condition(pdrNode, xmlMetaGood, None))
    Assert.assertTrue(checkAmm.ruleIgmgIgmrMatch.condition(pdrNode, xmlMetaBad, None))
  }

  def testRulePreIntPostIntMotRetLett2(): Unit = { //21
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <matr_mis>1234aaabbb1234</matr_mis>
      </Pre-int>
      <Post-int>
        <matr_mis>1234aaabbb1234</matr_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <matr_mis></matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPostIntMotRetLett2.condition(pdrNodeBadValued, null, None))
    Assert.assertTrue(checkAmm.rulePreIntPostIntMotRetLett2.condition(pdrNodeBadValued2, null, None))
    Assert.assertFalse(checkAmm.rulePreIntPostIntMotRetLett2.condition(pdrNodeGoodValued, null, None))
  }

  def testRulePreIntPostIntMotRetLett3(): Unit = { //22
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>3</mot_ret_lett>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>3</mot_ret_lett>
      <Pre-int>
        <matr_mis></matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <mot_ret_lett>3</mot_ret_lett>
      <Pre-int>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPostIntMotRetLett3.condition(pdrNodeBadValued, null, None))
    Assert.assertTrue(checkAmm.rulePreIntPostIntMotRetLett3.condition(pdrNodeBadValued2, null, None))
    Assert.assertFalse(checkAmm.rulePreIntPostIntMotRetLett3.condition(pdrNodeGoodValued, null, None))
  }

  def testRuleCauIntMisPreIntMatrMis(): Unit = { //23
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <matr_mis>1234aaabbb1234</matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <matr_mis></matr_mis>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue( checkAmm.ruleCauIntMisPreIntMatrMis.condition(pdrNodeBadValued,null,None) )
    Assert.assertTrue( checkAmm.ruleCauIntMisPreIntMatrMis.condition(pdrNodeBadValued2,null,None) )
    Assert.assertFalse( checkAmm.ruleCauIntMisPreIntMatrMis.condition(pdrNodeGoodValued,null,None) )
  }

  def testRulePreIntPreConvGruppoMisInt(): Unit = { //ID 24
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int></gruppo_mis_int>
      </Pre-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>NO</pre_conv>
      </Pre-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvNoGruppoMisInt.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvNoGruppoMisInt.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvNoGruppoMisInt.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePreIntPreConvGruppoMisInt2(): Unit = { // ID 25
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv></pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>

        <gruppo_mis_int>NO</gruppo_mis_int>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisIntNo.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvGruppoMisIntNo.condition(pdrNodeGoodValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvGruppoMisIntNo.condition(pdrNodeBadValued2,null,None))
  }

  def testRulePreIntPreConvLetCorrettore(): Unit = { //id 26
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <let_correttore>000000000000133</let_correttore>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>SI</pre_conv>
        <let_correttore></let_correttore>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <pre_conv>SI</pre_conv>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvSiLetCorr.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvSiLetCorr.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvSiLetCorr.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePreIntCoeffCorr(): Unit = { //id 27
    val paramX = 10

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("X"-> paramX.toString)
    )

    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramX-1).toString}</coeff_corr>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramX+1).toString}</coeff_corr>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntCoeffCorrIGMR.condition(pdrNodeBadValued,null,Option(ruleParameters)))
    Assert.assertFalse(checkAmm.rulePreIntCoeffCorrIGMR.condition(pdrNodeGoodValued,null,Option(ruleParameters)))

  }

  def testRulePostIntCoeffCorr(): Unit = { //id 28
    val paramX = 10

    val ruleParameters = RuleParameters(
      isActive = true
      , bloccante=true
      , parameters = Map("X"-> paramX.toString)
    )

    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <coeff_corr>{(paramX-1).toString}</coeff_corr>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <coeff_corr>{(paramX+1).toString}</coeff_corr>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostIntCoeffCorrIGMR.condition(pdrNodeBadValued,null,Option(ruleParameters)))
    Assert.assertFalse(checkAmm.rulePostIntCoeffCorrIGMR.condition(pdrNodeGoodValued,null,Option(ruleParameters)))

  }

  def testRulePreIntPreConvCauIntMatrConv(): Unit = { //id 29
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <cau_int_cor>valued</cau_int_cor>
        <pre_conv>SI</pre_conv>
        <matr_conv>valued</matr_conv>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <cau_int_cor>valued</cau_int_cor>
        <pre_conv>SI</pre_conv>
        <matr_conv></matr_conv>
      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <cau_int_cor>valued</cau_int_cor>
        <pre_conv>SI</pre_conv>

      </Pre-int>
      <Post-int></Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreIntPreConvSiCauIntCorr.condition(pdrNodeBadValued,null,None))
    Assert.assertTrue(checkAmm.rulePreIntPreConvSiCauIntCorr.condition(pdrNodeBadValued2,null,None))
    Assert.assertFalse(checkAmm.rulePreIntPreConvSiCauIntCorr.condition(pdrNodeGoodValued,null,None))
  }

  def testRulePostInitCauIntMisMatrMis(): Unit = { // id 30
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis></matr_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis>aaaaaaaaa</matr_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisMatrMisIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisMatrMisIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisMatrMis2(): Unit = { // id 31
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis>valued</matr_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <matr_mis></matr_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisMatrMis2IGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisMatrMis2IGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisTipoMis(): Unit = { // id 32
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>valued</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisTipoMisIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisTipoMisIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitTipoMisCauInitMisTelegestione(): Unit = { // id 33
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
        <telegestione>SI</telegestione>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <tipo_mis>02</tipo_mis>
        <telegestione></telegestione>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitTipoMisCauInitMisTelegestioneIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitTipoMisCauInitMisTelegestioneIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvGruppoMis(): Unit = { // id 34
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv></pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvGruppoMisIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvGruppoMisIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvGruppoMisInt(): Unit = { // id 35
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int>NO</gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
        <gruppo_mis_int></gruppo_mis_int>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvGruppoMisIntIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvGruppoMisIntIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvLetCorrettore(): Unit = { // id 36
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <let_correttore>000000000000001</let_correttore>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <let_correttore></let_correttore>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvLetCorrettoreIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvLetCorrettoreIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitPreConvMatrConv(): Unit = { // id 37
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <matr_conv>NO</matr_conv>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
        <matr_conv></matr_conv>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitPreConvMatrConvIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitPreConvMatrConvIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisClasseGruppoMis(): Unit = { // id 38
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis></classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G4</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisClasseGruppoMisIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisClasseGruppoMisIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitClasseGruppoMis(): Unit = { // id 39
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G1,6</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G2,55</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitClasseGruppoMisIGMR.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.rulePostInitClasseGruppoMisIGMR.condition(pdrNodeGoodValued,null,None))

  }

  def testRulePostInitCauIntMisClasseGruppoMisCode(): Unit = { // id 40
    val pdrNodeGoodValued = <DatiPdR>
      <cau_int_mis></cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis>G1,6</classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>
        <classe_gruppo_mis></classe_gruppo_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued2 = <DatiPdR>
      <cau_int_mis>1</cau_int_mis>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int></Pre-int>
      <Post-int>

      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePostInitCauIntMisClasseGruppoMisCodeIGMR.condition(pdrNodeBadValued, null, None))
    Assert.assertTrue(checkAmm.rulePostInitCauIntMisClasseGruppoMisCodeIGMR.condition(pdrNodeBadValued2, null, None))
    Assert.assertFalse(checkAmm.rulePostInitCauIntMisClasseGruppoMisCodeIGMR.condition(pdrNodeGoodValued, null, None))

  }


  def testRulePostInitCauIntMisAccMis(): Unit = { // id 41
    val pdrNodeGoodValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <acc_mis></acc_mis>
      </Post-int>
    </DatiPdR>

    val pdrNodeBadValued = <DatiPdR>
      <mot_ret_lett>2</mot_ret_lett>
      <cau_int_mis></cau_int_mis>
      <Pre-int></Pre-int>
      <Post-int>
        <acc_mis>3</acc_mis>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.ruleCauIntMisPostIntAccMis.condition(pdrNodeBadValued,null,None))
    Assert.assertFalse(checkAmm.ruleCauIntMisPostIntAccMis.condition(pdrNodeGoodValued,null,None))

  }

  def testRuleCoeffCorr(): Unit = { //42

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
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued2 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued3 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMinimo - 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMinimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued4 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued5 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued6 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo + 1).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued7 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr></coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued8 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
        <coeff_corr></coeff_corr>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued9 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
      </Pre-int>
      <Post-int>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued10 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Pre-int>
      <Post-int>
      </Post-int>
    </DatiPdr>

    val pdrNodeBadValued11 = <DatiPdr>
      <mot_ret_lett>2</mot_ret_lett>
      <Pre-int>
      </Pre-int>
      <Post-int>
        <coeff_corr>{(paramXMassimo).toString}</coeff_corr>
      </Post-int>
    </DatiPdr>

    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued1, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued2, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued3, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued4, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued5, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued6, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued7, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued8, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued9, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued10, null, None))
    Assert.assertTrue(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeBadValued11, null, None))

    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeGoodValued1, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeGoodValued2, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeGoodValued3, null, None))
    Assert.assertFalse(checkAmm.ruleCoeffCorrIGMR.condition(pdrNodeGoodValued4, null, None))

  }

  def testRulePreConvCauIntCorMotRettIGMR(): Unit = { // id 43
    //Bad node, error on Pre-int
    val pdrNodeBadPre =
      <DatiPdR>
        <cau_int_cor>SOSTITUZIONE</cau_int_cor>
        <mot_ret_lett>1</mot_ret_lett>
        <Pre-int>
          <pre_conv>NO</pre_conv>
        </Pre-int>
      </DatiPdR>

    //Bad node, error on Post-int
    val pdrNodeBadPost =
      <DatiPdR>
        <cau_int_cor>RIPRISTINO</cau_int_cor>
        <mot_ret_lett>2</mot_ret_lett>
        <Post-int>
          <pre_conv>NO</pre_conv>
        </Post-int>
    </DatiPdR>

    //Bad node, error on both
    val pdrNodeBadBoth = <DatiPdR>
      <cau_int_cor>SOSTITUZIONE</cau_int_cor>
      <mot_ret_lett>1</mot_ret_lett>
      <Pre-int>
        <pre_conv>NO</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>NO</pre_conv>
      </Post-int>
    </DatiPdR>

    //Good nodes
    // mot_ret_lett = 3
    val pdrNodeGoodMot3 =
      <DatiPdR>
        <cau_int_cor>SOSTITUZIONE</cau_int_cor>
        <mot_ret_lett>3</mot_ret_lett>
        <Pre-int>
          <pre_conv>NO</pre_conv>
        </Pre-int>
        <Post-int>
          <pre_conv>NO</pre_conv>
        </Post-int>
      </DatiPdR>

    // cau_int_cor not valued
    val pdrNodeGoodNoTrigger =
      <DatiPdR>
        <cau_int_cor></cau_int_cor>
        <mot_ret_lett>1</mot_ret_lett>
        <Pre-int>
          <pre_conv>NO</pre_conv>
        </Pre-int>
      </DatiPdR>

    //pre_conv ≠ NO on both
    val pdrNodeGoodPrePostSI = <DatiPdR>
      <cau_int_cor>SOSTITUZIONE</cau_int_cor>
      <mot_ret_lett>1</mot_ret_lett>
      <Pre-int>
        <pre_conv>SI</pre_conv>
      </Pre-int>
      <Post-int>
        <pre_conv>SI</pre_conv>
      </Post-int>
    </DatiPdR>

    Assert.assertTrue(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeBadPre,null,None))
    Assert.assertTrue(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeBadPost,null,None))
    Assert.assertTrue(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeBadBoth,null,None))
    Assert.assertFalse(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeGoodMot3,null,None))
    Assert.assertFalse(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeGoodNoTrigger,null,None))
    Assert.assertFalse(checkAmm.rulePreConvCauIntCorMotRettIGMR.condition(pdrNodeGoodPrePostSI,null,None))
  }
}
