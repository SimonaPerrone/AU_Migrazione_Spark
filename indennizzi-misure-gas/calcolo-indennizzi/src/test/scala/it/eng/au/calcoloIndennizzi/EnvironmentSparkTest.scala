package it.eng.au.calcoloIndennizzi

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOG:", "src/test/resources/params.properties", needsKryo = true, isLocal = true)
}
