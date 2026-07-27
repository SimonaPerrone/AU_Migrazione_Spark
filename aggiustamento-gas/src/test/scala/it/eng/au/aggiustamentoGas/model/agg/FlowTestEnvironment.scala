package it.eng.au.aggiustamentoGas.model.agg

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tml}
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class FlowTestEnvironment extends EnvironmentSparkTest {

  def testOrdering(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

    val tml4 = Tml(service = "TML",
      pdr = "03340007817247",
      date = Some(formatter.parseDateTime("29/03/2020")),
      measure = None,
      converted = None,
      serialNumberMis = None,
      pivaDistr = None,
      pivaUtente = None,
      serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_01177760491/DISTRIBUTORE/TMG_01177760491_06655971007/2021/0420/01177760491_06655971007_202003_TML_20210419151252_4_M.zip"),
      dataCaricamento = None,
      freqLet = None,
      readType = None,
      coefCorr = None,
      isValid = None)

    val tml21 = Tml(service = "TML",
      pdr = "03340007817247",
      date = Some(formatter.parseDateTime("29/03/2020")),
      measure = None,
      converted = None,
      serialNumberMis = None,
      pivaDistr = None,
      pivaUtente = None,
      serialNumberConv = None,
      localFile = Some("/mnt/isilonshare_gas/TMG_01177760491/DISTRIBUTORE/TMG_01177760491_06655971007/2021/0420/01177760491_06655971007_202003_TML_20210419151252_21_M.zip"),
      dataCaricamento = None,
      freqLet = None,
      readType = None,
      coefCorr = None,
      isValid = None)

    val sortedList = List(tml21, tml4).sorted(Flow.orderingSameDayFlows)
    println(tml21.dateLoadFromLocalFile)
    println(tml21.timestampLocalFile)
    println(tml21.progressiveLocalFile)
    println(tml4.dateLoadFromLocalFile)
    println(tml4.timestampLocalFile)
    println(tml4.progressiveLocalFile)
    println(tml4.fileName)
    Assert.assertEquals(tml4, sortedList.head)
    Assert.assertEquals(tml21, sortedList.last)
  }
}
