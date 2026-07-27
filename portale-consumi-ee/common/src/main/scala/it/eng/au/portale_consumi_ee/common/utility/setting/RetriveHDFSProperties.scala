package it.eng.au.portale_consumi_ee.common.utility.setting
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Properties
import java.io.InputStream


class RetriveHDFSProperties {

  /**
   * Load a properties file from HDFS.
   *
   * @param filePath The HDFS path to the properties file.
   * @return A Properties object containing the key-value pairs.
   */
  def loadPropertiesFromHDFS(filePath: String): Properties = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val properties = new Properties()
    val path = new Path(filePath)

    var inputStream: InputStream = null
    try {
      inputStream = fs.open(path)
      properties.load(inputStream)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(s"Failed to load properties from HDFS at $filePath", e)
    } finally {
      if (inputStream != null) inputStream.close()
    }

    properties
  }

}
