package it.sferanet.au

import it.sferanet.au.utilities.Environment
import junit.framework.TestCase

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "src/test/resources/config.properties", isLocal = true)
}
