package it.eng.au.eng.utility

import junit.framework.TestCase
import it.eng.au.utility.environment.Environment


trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test",true)
}