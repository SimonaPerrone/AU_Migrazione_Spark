package it.au.misure.eng.ammissibilita.pod

import java.io.File

import it.au.misure.eng.model.XMLMetadata
import it.au.misure.eng.utility.SystemUtility
import junit.framework.TestCase
import org.junit.Assert

class TestCheckAmmissibilitaPodRules extends TestCase {
  SystemUtility.setLocalLaunch()

  @deprecated("Use CheckAmmissibilitaFileRules.ruleDatiFile instead and TestCheckAmmissibilitaFileRules.ruleDatiFile","11/01/2021")
  def testRuleDatiFile(): Unit = {
    val datiPod2018 =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2018</MeseAnno>
      </DatiPod>

    val datiPod2020 =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2020</MeseAnno>
      </DatiPod>

    val datiPod2020MinusMonth =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>09/2020</MeseAnno>
      </DatiPod>

    val datiPod2020DataMisura2020 =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2020</MeseAnno>
        <DataMisura>05/10/2020</DataMisura>
      </DatiPod>

    val datiPod2020DataMisura2020MinusMonth =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2020</MeseAnno>
        <DataMisura>05/09/2020</DataMisura>
      </DatiPod>

    val datiPod2020DataMisura2019 =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>10/2020</MeseAnno>
        <DataMisura>05/10/2019</DataMisura>
      </DatiPod>

    val xmlWithMeta2020 = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2018, xmlWithMeta2020, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2020, xmlWithMeta2020, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2020MinusMonth, xmlWithMeta2020, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2020DataMisura2020, xmlWithMeta2020, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2020DataMisura2020MinusMonth, xmlWithMeta2020, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleDatiFile.condition(datiPod2020DataMisura2019, xmlWithMeta2020, None))
  }

  def testRuleDataMisura(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataMisura>05/09/2020</DataMisura>
        <DatiPdp>
          <Trattamento>F</Trattamento>
        </DatiPdp>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataMisura></DataMisura>
        <DatiPdp>
          <Trattamento>F</Trattamento>
        </DatiPdp>
      </DatiPod>

    val datiPodTrattamentoToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataMisura></DataMisura>
        <DatiPdp>
          <Trattamento>A</Trattamento>
        </DatiPdp>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataMisura.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleDataMisura.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataMisura.condition(datiPodTrattamentoToNotControl, xmlWithMeta, None))
  }

  def testRuleMeseAnno(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno>09/2020</MeseAnno>
        <DatiPdp>
          <Trattamento>O</Trattamento>
        </DatiPdp>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno></MeseAnno>
        <DatiPdp>
          <Trattamento>O</Trattamento>
        </DatiPdp>
      </DatiPod>

    val datiPodTrattamentoToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <MeseAnno></MeseAnno>
        <DatiPdp>
          <Trattamento>A</Trattamento>
        </DatiPdp>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMeseAnno.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleMeseAnno.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMeseAnno.condition(datiPodTrattamentoToNotControl, xmlWithMeta, None))
  }

  def testRuleCodPrat(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <CodPrat_SII>valued</CodPrat_SII>
        <Curva>
          <Raccolta>S</Raccolta>
        </Curva>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Curva>
          <Raccolta>S</Raccolta>
        </Curva>
      </DatiPod>

    val datiPodRaccoltaToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <CodPrat_SII></CodPrat_SII>
        <Curva>
          <Raccolta>no</Raccolta>
        </Curva>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodPrat.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodPrat.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodPrat.condition(datiPodRaccoltaToNotControl, xmlWithMeta, None))
  }

  def testRuleDataPrest1(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Curva>
          <Raccolta>S</Raccolta>
        </Curva>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05-10-2020</DataPrest>
        <Curva>
          <Raccolta>S</Raccolta>
        </Curva>
      </DatiPod>

    val datiPodRaccoltaToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest></DataPrest>
        <Curva>
          <Raccolta>no</Raccolta>
        </Curva>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataPrest1.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleDataPrest1.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataPrest1.condition(datiPodRaccoltaToNotControl, xmlWithMeta, None))
  }

  def testRuleDataPrest2(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <TipoRettifica>S</TipoRettifica>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05-10-2020</DataPrest>
        <TipoRettifica>S</TipoRettifica>
      </DatiPod>

    val datiPodTipoRettificaToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest></DataPrest>
        <TipoRettifica>no</TipoRettifica>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RFO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataPrest2.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleDataPrest2.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleDataPrest2.condition(datiPodTipoRettificaToNotControl, xmlWithMeta, None))
  }

  def testRuleKaKrKp(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Curva>
          <Ka>S</Ka>
          <Kr>S</Kr>
          <Kp>S</Kp>
        </Curva>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Curva>
          <Ka>S</Ka>
          <Kr>S</Kr>
        </Curva>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "VNO2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleKaKrKp.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleKaKrKp.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleKaKrKp.condition(datiPodValued, xmlWithMetaNoControl, None))
  }

  def testRuleMotivazioneStima(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Misura>
          <MotivazioneStima>valued</MotivazioneStima>
        </Misura>
        <Curva>
          <TipoDato>S</TipoDato>
        </Curva>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Misura>
          <MotivazioneStima></MotivazioneStima>
        </Misura>
        <Curva>
          <TipoDato>S</TipoDato>
        </Curva>
      </DatiPod>

    val datiPodTipoDatoToNotControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Misura>
          <MotivazioneStima></MotivazioneStima>
        </Misura>
        <Curva>
          <TipoDato>no</TipoDato>
        </Curva>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "PDO",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PDO"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMotivazioneStima.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleMotivazioneStima.condition(datiPodNotValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMotivazioneStima.condition(datiPodTipoDatoToNotControl, xmlWithMeta, None))
  }

  def testRuleTrattamento(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DatiPdp>
          <Trattamento>M</Trattamento>
        </DatiPdp>
      </DatiPod>


    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DatiPdp>
          <Trattamento>O</Trattamento>
        </DatiPdp>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "PNO2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamento.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleTrattamento.condition(datiPodBadValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamento.condition(datiPodBadValued, xmlWithMetaNoControl, None))

    val vno = <DatiPod>
      <Pod>IT029E00001106</Pod>
      <DataMisura>01/03/2021</DataMisura>
      <DataPrest>04/03/2021</DataPrest>
      <CodPrat_SII>SII202112852988</CodPrat_SII>
      <DatiPdp>
        <Trattamento>O</Trattamento>
        <Tensione>400</Tensione>
        <Forfait>NO</Forfait>
        <GruppoMis>SI</GruppoMis>
        <Ka>1,000</Ka>
        <Kr>1,000</Kr>
        <Kp>1,000</Kp>
      </DatiPdp>
      <Misura xsi:type="DettaglioMisuraNOv2Type">
        <Raccolta>V</Raccolta>
        <TipoDato>E</TipoDato>
        <Validato>S</Validato>
      </Misura>
    </DatiPod>
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleTrattamento.condition(vno, xmlWithMeta, None))

  }

  def testRuleCodFlussoRaccolta(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Curva>
          <Raccolta>V</Raccolta>
        </Curva>
      </DatiPod>


    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <Curva>
          <Raccolta>P</Raccolta>
        </Curva>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "pdo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "VNO"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoRaccolta.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoRaccolta.condition(datiPodBadValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoRaccolta.condition(datiPodBadValued, xmlWithMetaNoControl, None))
  }

  def testRuleCodFlussoTipoRettifica(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <TipoRettifica>V</TipoRettifica>
      </DatiPod>

    val datiPodNotValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05-10-2020</DataPrest>
        <TipoRettifica></TipoRettifica>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05-10-2020</DataPrest>
        <TipoRettifica>BAD</TipoRettifica>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodBadValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodNotValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodValued, xmlWithMetaNoControl, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodBadValued, xmlWithMetaNoControl, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoTipoRettifica.condition(datiPodNotValued, xmlWithMetaNoControl, None))
  }

  def testRuleCodFlussoPotMax(): Unit = {

    val datiPotValued =
      <DettaglioCurva>
        <PotMax>presente</PotMax>
      </DettaglioCurva>

    val datiPotNotValued =
      <DettaglioCurva>
        <PotMax></PotMax>
      </DettaglioCurva>

    val datiPotNotPresent =
      <DettaglioCurva>
        <Other></Other>
      </DettaglioCurva>

    val datiPotNotPresentMotivazioneEquals3 =
      <DatiPod>
        <Motivazione>3</Motivazione>
        <DettaglioCurva>
          <Other></Other>
        </DettaglioCurva>
      </DatiPod>
    val datiPotNotPresentMotivazioneEquals2 =
      <DatiPod>
        <Motivazione>2</Motivazione>
        <DettaglioCurva>
          <Other></Other>
        </DettaglioCurva>
      </DatiPod>
    val datiPotPresentMotivazioneEquals3 =
      <DatiPod>
        <Motivazione>3</Motivazione>
        <DettaglioCurva>
          <PotMax>presente</PotMax>
        </DettaglioCurva>
      </DatiPod>
    val datiPotPresentMotivazioneEquals2 =
      <DatiPod>
        <Motivazione>2</Motivazione>
        <DettaglioCurva>
          <PotMax>presente</PotMax>
        </DettaglioCurva>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "VNO2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )
    val xmlWithMetaRett = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "Rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RFO"
    )

    // corretto
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotValued, xmlWithMeta, None))
    //errore: tag assente
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotPresent, xmlWithMeta, None))
    //errore: tag vuoto
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotValued, xmlWithMeta, None))
    //se il flusso non e' tra gli indicati non c'e' errore
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotValued, xmlWithMetaNoControl, None))

    //Se il flusso é rettifica e motivazione 3 non si applica la regola
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotPresentMotivazioneEquals3, xmlWithMetaRett, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotPresentMotivazioneEquals3, xmlWithMetaRett, None))
    //Se il flusso é rettifica e motivazione != 3  si applica la regola
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotPresentMotivazioneEquals2, xmlWithMetaRett, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotPresentMotivazioneEquals2, xmlWithMetaRett, None))
    //se il flusso non é tra gli specificati non si applica la regola
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotPresentMotivazioneEquals2, xmlWithMetaRett.copy(codFlusso = "SNM"), None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleCodFlussoPotMax.condition(datiPotNotPresentMotivazioneEquals2, xmlWithMetaRett.copy(codFlusso = "SNM"), None))
  }

  def testRuleTipoRettificaMotivazione(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <TipoRettifica>V</TipoRettifica>
        <Motivazione>6</Motivazione>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <TipoRettifica>V</TipoRettifica>
        <Motivazione>3</Motivazione>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rettifica",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTipoRettificaMotivazione.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleTipoRettificaMotivazione.condition(datiPodBadValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTipoRettificaMotivazione.condition(datiPodValued, xmlWithMetaNoControl, None))

  }

  def testRuleTrattamentoConsumo(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>O</Trattamento>
        <Consumo></Consumo>
      </DatiPod>

    val datiPodNoControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>M</Trattamento>
        <Consumo>nonempty</Consumo>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>O</Trattamento>
        <Consumo>nonempty</Consumo>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "pno",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumo.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumo.condition(datiPodNoControl, xmlWithMeta, None))

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleTrattamentoConsumo.condition(datiPodBadValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumo.condition(datiPodValued, xmlWithMetaNoControl, None))

  }

  def testRuleTrattamentoConsumoCurva(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>O</Trattamento>
        <Consumo></Consumo>
        <Curva></Curva>
      </DatiPod>

    val datiPodNoControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>M</Trattamento>
        <Consumo>nonempty</Consumo>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Trattamento>O</Trattamento>
        <Consumo>nonempty</Consumo>
        <Curva>nonempty</Curva>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "pno",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumoCurva.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumoCurva.condition(datiPodNoControl, xmlWithMeta, None))

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleTrattamentoConsumoCurva.condition(datiPodBadValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleTrattamentoConsumoCurva.condition(datiPodValued, xmlWithMetaNoControl, None))

  }

  def testRuleForfaitConsumo(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Forfait>SI</Forfait>
        <Consumo>nonempty</Consumo>
        <Curva></Curva>
      </DatiPod>

    val datiPodNoControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Forfait>NO</Forfait>
        <Consumo>nonempty</Consumo>
        <Curva></Curva>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Forfait>SI</Forfait>
        <Consumo></Consumo>
        <Curva></Curva>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "pno",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleForfaitConsumo.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleForfaitConsumo.condition(datiPodNoControl, xmlWithMeta, None))

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleForfaitConsumo.condition(datiPodBadValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleForfaitConsumo.condition(datiPodValued, xmlWithMetaNoControl, None))

  }

  def testRuleMotivazioneMisuraConsumo(): Unit = {

    val datiPodValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Motivazione>3</Motivazione>
      </DatiPod>


    val datiPodNoControl =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Motivazione>2</Motivazione>
        <Misura>nonempty</Misura>
        <Consumo>nonempty</Consumo>
      </DatiPod>

    val datiPodBadValued =
      <DatiPod>
        <Pod>IT001E49161679</Pod>
        <DataPrest>05/10/2020</DataPrest>
        <Motivazione>3</Motivazione>
        <Misura>nonempty</Misura>
        <Consumo>nonempty</Consumo>
      </DatiPod>


    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    val xmlWithMetaNoControl = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "pno",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "noControl"
    )

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMotivazioneMisuraConsumo.condition(datiPodValued, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMotivazioneMisuraConsumo.condition(datiPodNoControl, xmlWithMeta, None))

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleMotivazioneMisuraConsumo.condition(datiPodBadValued, xmlWithMeta, None))

    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleMotivazioneMisuraConsumo.condition(datiPodValued, xmlWithMetaNoControl, None))

  }

  def testRuleEaEr(): Unit = {

    val datiPodBadValuedDst1 =
      <DatiPod>
        <Misura>
          <Raccolta>P</Raccolta>
          <TipoDato>E</TipoDato>
          <Validato>S</Validato>
          <PotMax>3,232</PotMax>
          <Ea Dst="1" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018" E9="0,042" E10="0,029"
              E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031"
              E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029"
              E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042"
              E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025"
              E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057"
              E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055"
              E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121"
              E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107"
              E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="1" E1="0,015" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018" E9="0,042" E10="0,029"
              E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031"
              E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029"
              E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042"
              E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025"
              E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057"
              E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055"
              E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121"
              E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107"
              E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
        </Misura>
      </DatiPod>

    val datiPodGoodValuedDst1 =
      <DatiPod>
        <Misura>
          <Ea Dst="1" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018"
              E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031"
              E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029"
              E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042"
              E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025"
              E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057"
              E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055"
              E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121"
              E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107"
              E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="1" E1="0,015" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018"
              E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031"
              E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029"
              E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042"
              E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025"
              E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057"
              E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055"
              E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121"
              E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107"
              E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
        </Misura>
      </DatiPod>

    val datiPodBadValuedDst2 =
      <DatiPod>
        <Misura>
          <Ea Dst="2" E1="0,015" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018" E9="0,010" E10="0,010" E11="0,010"
              E13="">02</Ea>
          <Er Dst="2" E1="0,000" E2="0,000" E3="0,000" E4="0,000" E5="0,000" E6="0,000" E7="0,000" E8="0,000" E9="0,00" E10="0,00" E11="0,01" E12="1.2">02</Er>
        </Misura>
      </DatiPod>

    val datiPodGoodValuedDst2 =
      <DatiPod>
        <Misura>
          <Ea Dst="2" E1="0,015" E2="0,010" E3="0,011" E4="0,016" E5="0,013" E6="0,010" E7="0,010" E8="0,018" E9="0,00" E10="0,00" E11="0,01" E12="0,00">02</Ea>
          <Er Dst="2" E1="0,000" E2="0,000" E3="0,000" E4="0,000" E5="0,000" E6="0,000" E7="0,000" E8="0,000" E9="0,00" E10="0,00" E11="0,01" E12="1.2">02</Er>
        </Misura>
      </DatiPod>

    val datiPodBadValuedDst3 =
      <DatiPod>
        <Misura>
          <Ea Dst="3" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="3" E9="" E10="" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
        </Misura>
      </DatiPod>

    val datiPodGoodValuedDst3 =
      <DatiPod>
        <Misura>
          <Ea Dst="3" E9="0,042" E10="0,029" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="3" E9="0,042" E10="0,029" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
        </Misura>
      </DatiPod>

    val datiPodMixed =
      <DatiPod>
        <Misura>
          <Ea Dst="3" E9="0,042" E10="0,029" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="3" E9="0,042" E10="0,029" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
          <Ea Dst="3" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Ea>
          <Er Dst="3" E9="" E10="" E11="0,029" E12="0,029" E13="0,029" E14="0,033" E15="0,054" E16="0,024" E17="0,036" E18="0,034" E19="0,032" E20="0,031" E21="0,044" E22="0,030" E23="0,030" E24="0,029" E25="0,030" E26="0,029" E27="0,043" E28="0,029" E29="0,029" E30="0,029" E31="0,029" E32="0,028" E33="0,040" E34="0,031" E35="0,028" E36="0,029" E37="0,029" E38="0,028" E39="0,029" E40="0,042" E41="0,029" E42="0,028" E43="0,029" E44="0,028" E45="0,029" E46="0,042" E47="0,029" E48="0,034" E49="0,039" E50="0,025" E51="0,036" E52="0,046" E53="0,032" E54="0,053" E55="0,076" E56="0,076" E57="0,062" E58="0,069" E59="0,056" E60="0,057" E61="0,056" E62="0,055" E63="0,068" E64="0,056" E65="0,055" E66="0,054" E67="0,055" E68="0,069" E69="0,053" E70="0,055" E71="0,053" E72="0,055" E73="0,068" E74="0,055" E75="0,056" E76="0,056" E77="0,079" E78="0,161" E79="0,114" E80="0,121" E81="0,110" E82="0,115" E83="0,127" E84="0,101" E85="0,113" E86="0,108" E87="0,122" E88="0,107" E89="0,107" E90="0,107" E91="0,110" E92="0,115" E93="0,105" E94="0,106" E95="0,103" E96="0,118">02</Er>
        </Misura>
      </DatiPod>

    val xmlWithMeta = new XMLMetadata(
      file = new File(""),
      pivaDistributore = "",
      pivaUDD = "",
      annoMese = "202010",
      flusso = "rfo",
      timestamp = "",
      progressivo = "",
      codDp = "",
      sm = "",
      codFlusso = "RNV2G"
    )

    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodBadValuedDst1, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodGoodValuedDst1, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodBadValuedDst2, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodGoodValuedDst2, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodBadValuedDst3, xmlWithMeta, None))
    Assert.assertFalse(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodGoodValuedDst3, xmlWithMeta, None))
    Assert.assertTrue(CheckAmmissibilitaPodRules.ruleEaEr.condition(datiPodMixed, xmlWithMeta, None))


  }

}
