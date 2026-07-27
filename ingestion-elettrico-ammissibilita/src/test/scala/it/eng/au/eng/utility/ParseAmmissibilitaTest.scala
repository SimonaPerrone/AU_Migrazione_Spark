package it.eng.au.eng.utility

import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod.parseAmmissibilitaArgs
import org.junit.Assert

class ParseAmmissibilitaTest extends EnvironmentSparkTest {
  def testParseAmmissibilitaArgs(): Unit = {
    val args = Array("-g", "--anno", "2022", "--mese", "2", "--giorno", "12")

    val result = parseAmmissibilitaArgs(args)

    Assert.assertEquals("1G", result.g)
    Assert.assertEquals(false, result.isSmis)
    Assert.assertEquals("2022", result.year)
    Assert.assertEquals("02", result.month)
    Assert.assertEquals("12", result.day)
  }
}
