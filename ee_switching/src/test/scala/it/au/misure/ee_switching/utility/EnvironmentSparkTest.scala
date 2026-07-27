package it.au.misure.ee_switching.utility

import junit.framework.TestCase
import it.au.misure.ee_switching.utility.environment.Environment


trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test",true)
}
