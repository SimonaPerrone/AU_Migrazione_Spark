package it.au.misure.ee_switching.args

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import it.au.misure.ee_switching.utility.Constants.{FUNZIONALI, STORICI}
import it.au.misure.ee_switching.utility.EnvironmentSparkTest
import org.junit.{Assert, Test}


class TestFlowArgsFactory extends EnvironmentSparkTest {

  def assertThrows[E](f: => Unit)(implicit eType:scala.reflect.ClassTag[E]): Unit = {
    try {
      f
    } catch {
      case e: Exception =>
        if ( eType.runtimeClass.isAssignableFrom(e.getClass))
          return;
    }
    throw new AssertionError("Expected error of type " + eType.runtimeClass.getName )
  }

  @Test
  def testGetInputArgs(): Unit = {
    val argsPath: String = "src/test/resources/files/args/"

    val flowArgsMeta1 = FlowArgsMetadata(flowName = FUNZIONALI, runOrdinaria = true, listaPodFilePath = argsPath + "listaPod.txt")
    val flowArgsConfig1 = FlowArgsFactory.getInputArgs(flowArgsMeta1)
    Assert.assertEquals(8, flowArgsConfig1.listaPod.length)
    Assert.assertEquals(true, flowArgsConfig1.runOrdinaria)
    Assert.assertEquals(FUNZIONALI, flowArgsConfig1.flowName)
//    println(FlowArgsFactory.getInputArgs(flowArgsMeta1).toString)

    val flowArgsMeta2 = FlowArgsMetadata(flowName = STORICI, runOrdinaria = false, timestampFilePath = argsPath + "timestamp.txt")
    val flowArgsConfig2 = FlowArgsFactory.getInputArgs(flowArgsMeta2)
    Assert.assertEquals("2020-12-15 15:45:12.0", flowArgsConfig2.timestamp.toString)
    Assert.assertEquals(false, flowArgsConfig2.runOrdinaria)
    Assert.assertEquals(STORICI, flowArgsConfig2.flowName)

    val flowArgsMeta3 = FlowArgsMetadata(flowName = STORICI, runOrdinaria = false, timestampFilePath = argsPath + "timestampWrong.txt")
    assertThrows[IllegalArgumentException] { FlowArgsFactory.getInputArgs(flowArgsMeta3) }

    val flowArgsMeta4 = FlowArgsMetadata(flowName = FUNZIONALI, listaDistributoriFilePath = argsPath + "listaDistributori.txt")
    val flowArgsConfig4 = FlowArgsFactory.getInputArgs(flowArgsMeta4)
    Assert.assertEquals(9, flowArgsConfig4.listaDistributori.length)
    Assert.assertEquals(true, flowArgsConfig4.runOrdinaria)

    val flowArgsMeta5 = FlowArgsMetadata(flowName = FUNZIONALI, listaUddFilePath = argsPath + "listaUdd.txt")
    val flowArgsConfig5 = FlowArgsFactory.getInputArgs(flowArgsMeta5)
    Assert.assertEquals(6, flowArgsConfig5.listaUdd.length)

    val flowArgsMeta6 = FlowArgsMetadata(flowName = FUNZIONALI, listaCoppieDistrUddFilePath = argsPath + "listacoppiePIva.txt")
    val flowArgsConfig6 = FlowArgsFactory.getInputArgs(flowArgsMeta6)
    Assert.assertEquals(6, flowArgsConfig6.listaCoppieDistrUdd.length)

    val flowArgsMeta7 = FlowArgsMetadata(flowName = FUNZIONALI, listaDateFunzionaliSWFilePath = argsPath + "listaDateFunzionaliSW.txt")
    val flowArgsConfig7 = FlowArgsFactory.getInputArgs(flowArgsMeta7)
    Assert.assertEquals(4, flowArgsConfig7.listaDateSW.length)

    val flowArgsMeta8 = FlowArgsMetadata(flowName = FUNZIONALI, listaDateFunzionaliNAFilePath = argsPath + "listaDateFunzionaliNA.txt")
    val flowArgsConfig8 = FlowArgsFactory.getInputArgs(flowArgsMeta8)
    Assert.assertEquals(4, flowArgsConfig8.listaDateNA.length)

    val flowArgsMeta9 = FlowArgsMetadata(flowName = STORICI, listaDateStoriciSWFilePath = argsPath + "listaDateStoriciSW.txt")
    val flowArgsConfig9 = FlowArgsFactory.getInputArgs(flowArgsMeta9)
    Assert.assertEquals(4, flowArgsConfig9.listaDateSW.length)

    val flowArgsMeta10 = FlowArgsMetadata(flowName = FUNZIONALI, runOrdinaria = true, listaPodFilePath = argsPath + "listaPod.txt", listaDistributoriFilePath = argsPath + "listaDistributori.txt")
    val flowArgsConfig10 = FlowArgsFactory.getInputArgs(flowArgsMeta10)
    Assert.assertEquals(8, flowArgsConfig10.listaPod.length)
    Assert.assertEquals(9, flowArgsConfig10.listaDistributori.length)

  }

  @Test
  def testCheckInputArgs(): Unit = {

    val flowArgsConfig1 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateSW = Seq(LocalDate.now))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig1) }

    val flowArgsConfig2 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateNA = Seq(LocalDate.now))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig2) }

    val flowArgsConfig3 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaPod = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig3) }

    val flowArgsConfig4 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateSW = Seq(LocalDate.now), listaPod = Seq("00000000000000"), listaDistributori = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig4) }

    val flowArgsConfig5 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateSW = Seq(LocalDate.now), listaPod = Seq("00000000000000"), listaUdd = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig5) }

    val flowArgsConfig6 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateNA = Seq(LocalDate.now), listaDistributori = Seq("00000000000000"), listaUdd = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig6) }

    val flowArgsConfig7 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateNA = Seq(LocalDate.now), listaPod = Seq("00000000000000"), listaDistributori = Seq("00000000000000"), listaUdd = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig7) }

    val flowArgsConfig8 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, timestamp = Timestamp.valueOf(LocalDateTime.now) , listaDateSW = Seq(LocalDate.now), listaPod = Seq("00000000000000"), listaDistributori = Seq("00000000000000"), listaUdd = Seq("00000000000000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig8) }

    val flowArgsConfig9 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaCoppieDistrUdd = Seq(("000","000")), listaPod = Seq("000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig9) }

    val flowArgsConfig10 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaCoppieDistrUdd = Seq(("000","000")), listaDistributori = Seq("000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig10) }

    val flowArgsConfig11 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaCoppieDistrUdd = Seq(("000","000")), listaUdd = Seq("000"))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig11) }

    val flowArgsConfig12 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateNA = Seq(LocalDate.now, LocalDate.now, LocalDate.now))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig12) }

    val flowArgsConfig13 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateNA = Seq(LocalDate.now, LocalDate.now))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig13) }

    val flowArgsConfig14 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateNA = Seq(LocalDate.parse("2021-02-22"), LocalDate.parse("2021-03-22")))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig14) }

    val flowArgsConfig15 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateNA = Seq(LocalDate.now))
    assertThrows[IllegalArgumentException] { FlowArgsFactory.checkInputArgs(flowArgsConfig15) }

  }

}