package it.eng.au.cli

import it.eng.au.args.{AmmissibilitaParameters, ParseAmmissibilitaArgs}
import junit.framework.TestCase
import it.eng.au.utility.Constants
//import junit.framework.TestCase.assertEquals
import org.junit.Assert

class TestParseAmmissibilitaArgs extends TestCase {

  var args: Array[String] = _
  var ammissibilitaParam: AmmissibilitaParameters = _

  override def setUp {

    // The following replicates an example of the arguments passed using the commandline.
    args = Array(
      "-g",
      "-y", "2020",
      "-m", "09",
      "-d", "23",
      "-S"
    )

    ammissibilitaParam = ParseAmmissibilitaArgs.parse(args)
  }

  def testEquals {
    Assert.assertEquals(ammissibilitaParam, AmmissibilitaParameters(Constants._1G, "2020", "09","23", true))
  }
}
