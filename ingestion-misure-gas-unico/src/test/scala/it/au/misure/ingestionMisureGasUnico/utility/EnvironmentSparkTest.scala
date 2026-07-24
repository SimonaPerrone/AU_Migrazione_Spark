package it.au.misure.ingestionMisureGasUnico.utility

import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test",true)
}
