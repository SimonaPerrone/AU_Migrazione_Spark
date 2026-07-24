package it.au.misure.eng.ammissibilita.pod

import it.au.misure.eng.model.XMLMetadata
import it.au.misure.eng.utility.SystemUtility
import junit.framework.TestCase
import org.junit.Assert

import java.io.File

class TestCheckAmmissibilitaPodSMISRules extends TestCase {
  SystemUtility.setLocalLaunch()

  def testRuleMotivazione():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>04</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleMotivazione.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMotivazione.condition(xmlFileGood, null, None))

  }

  def testRuleDataMisura():Unit = {

    val xmlWithMetaGood = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "05724831002",
      pivaUDD = "",
      annoMese = "201811",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlWithMetaBad = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "05724831001",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    val xmlFile = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleDataMisura.condition(xmlFile, xmlWithMetaBad, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleDataMisura.condition(xmlFile, xmlWithMetaGood, None))

  }

  def testRuleTipoMisuratoreMontaggio():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleTipoMisuratoreMontaggio.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleTipoMisuratoreMontaggio.condition(xmlFileGood, null, None))

  }

  def testRulePodCompetenceDistributor(): Unit = {

    val xmlMetaTrue = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      isPodCompetenceDistr = true)

    val xmlMetaFalse = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      isPodCompetenceDistr = false)

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.rulePodCompetenceDistributor.condition(null, xmlMetaFalse, None)) //only one file pass
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.rulePodCompetenceDistributor.condition(null, xmlMetaTrue, None)) //at least one pass
  }

  def testRulePodCompetenceUdd(): Unit = {

    val xmlMetaTrue = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      isPodCompetenceUdd = true)

    val xmlMetaFalse = new XMLMetadata(
      file = null,
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "",
      flusso = "",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      isPodCompetenceUdd = false)

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.rulePodCompetenceUdd.condition(null, xmlMetaFalse, None)) //only one file pass
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.rulePodCompetenceUdd.condition(null, xmlMetaTrue, None)) //at least one pass
  }

  def testRuleDataMontaggioValorizzata():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G></DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood2 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>02</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleDataMontaggioValorizzata.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleDataMontaggioValorizzata.condition(xmlFileGood, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleDataMontaggioValorizzata.condition(xmlFileGood2, null, None))

  }

  def testRuleMisuratoreSmontaggioG():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2></PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood1 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood2 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>H</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioG.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioG.condition(xmlFileGood1, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioG.condition(xmlFileGood2, null, None))
  }

  def testRuleMisuratoreMontaggioG():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>1,234</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3></PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood1 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood2 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>H</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioG.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioG.condition(xmlFileGood1, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioG.condition(xmlFileGood2, null, None))
  }

  def testRuleMisuratoreSmontaggioT():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>2,125</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood1 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <EaM>2,345</EaM>
        <ErM>4,564</ErM>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
        <PotM>3.094</PotM>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    val xmlFileGood2 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>H</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioT.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioT.condition(xmlFileGood1, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreSmontaggioT.condition(xmlFileGood2, null, None))
  }

  def testRuleMisuratoreMontaggioT():Unit = {

    val xmlFileBad = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>1,234</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <EaM></EaM>
        <ErM>2.913</ErM>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3></PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
        <PotM>2.451</PotM>
      </Montaggio>
    </DatiPod>

    val xmlFileGood1 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>T</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <EaM>0.678</EaM>
        <ErM>3,095</ErM>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
        <PotM>2.451</PotM>
      </Montaggio>
    </DatiPod>

    val xmlFileGood2 = <DatiPod>
      <Pod>IT999E00000003</Pod>
      <Motivazione>01</Motivazione>
      <Smontaggio>
        <TipoMisuratore>H</TipoMisuratore>
        <DataMisura>29/11/2018</DataMisura>
        <TipoDato>E</TipoDato>
        <EaF1>2,456</EaF1>
        <EaF2>4,567</EaF2>
        <EaF3>1,003</EaF3>
        <EaF4>4,987</EaF4>
        <EaF5>0,565</EaF5>
        <EaF6>10,456</EaF6>
        <PotF1>0,763</PotF1>
        <PotF2>3,875</PotF2>
        <PotF3>2,454</PotF3>
        <PotF4>3,987</PotF4>
        <PotF5>1,009</PotF5>
        <PotF6>2,098</PotF6>
      </Smontaggio>
      <Montaggio>
        <TipoMisuratore>P</TipoMisuratore>
        <DataMisura>30/11/2018</DataMisura>
        <DataMessaRegime2G>01/01/2019</DataMessaRegime2G>
        <Tensione>220</Tensione>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>1</MatrAtt>
        <MatrRea>2</MatrRea>
        <MatrPot>3</MatrPot>
        <CifreAtt>33</CifreAtt>
        <CifreRea>44</CifreRea>
        <CifrePot>55</CifrePot>
        <EaF1>0,222</EaF1>
        <EaF2>1,444</EaF2>
        <EaF3>2,445</EaF3>
        <EaF4>3,567</EaF4>
        <EaF5>5,655</EaF5>
        <EaF6>0,550</EaF6>
        <PotF1>0,221</PotF1>
        <PotF2>1,009</PotF2>
        <PotF3>2,008</PotF3>
        <PotF4>3,005</PotF4>
        <PotF5>0,002</PotF5>
        <PotF6>1,221</PotF6>
      </Montaggio>
    </DatiPod>

    Assert.assertTrue(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioT.condition(xmlFileBad, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioT.condition(xmlFileGood1, null, None))
    Assert.assertFalse(CheckAmmissibilitaPodRulesSMIS.ruleMisuratoreMontaggioT.condition(xmlFileGood2, null, None))
  }
}
