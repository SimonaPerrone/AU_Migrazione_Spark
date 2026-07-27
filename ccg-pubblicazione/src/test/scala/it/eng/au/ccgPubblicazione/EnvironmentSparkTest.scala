package it.eng.au.ccgPubblicazione

import it.eng.au.ccgPubblicazione.utility.Environment
import junit.framework.TestCase
import org.apache.commons.io.FileUtils

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "src/test/resources/params.properties", true)
  Environment.setProperty("daterun", Timestamp.valueOf(LocalDateTime.now()).toString)
  FileUtils.deleteDirectory(new File(Environment.getIsilonBasepathOut))

}
