package pubblicazioneIndennizzi

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.utility.Properties
import junit.framework.TestCase
import org.apache.commons.io.FileUtils

import java.io.File

trait EnvironmentSparkTest extends TestCase {
  Environment.getOrCreate("Test", "LOG:", "src/test/resources/params.properties", isLocal = true)
  FileUtils.deleteDirectory(new File(Properties.getIsilonBasepathOut))
}
