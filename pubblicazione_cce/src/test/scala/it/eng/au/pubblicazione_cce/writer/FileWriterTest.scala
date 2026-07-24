package it.eng.au.pubblicazione_cce.writer

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.file.writer.FileWriter
import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.junit.Assert

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class FileWriterTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  def testWrite(): Unit = {

    val userDir = System.getProperty("user.dir")
    val filePathRoot = s"${userDir}/target/test/"
    val filePathSubDirectories = s"writer/FileWriter/"
    val fileName = "test_file1.csv"
    val fileFullName = filePathRoot + filePathSubDirectories + fileName
    val fileContent = "a,b,c\n1,2,3"

    val fileModel = Seq(FileModel(
      filePathRoot = filePathRoot,
      filePathSubDirectories = Some(filePathSubDirectories),
      fileName = fileName,
      fileFullName = fileFullName,
      fileContent = fileContent.getBytes
    )).toDS()

    new FileWriter().write(fileModel)

    val result = new File(fileFullName)

    Assert.assertTrue(result.exists())
    Assert.assertEquals(fileContent, new String(Files.readAllBytes(result.toPath), StandardCharsets.UTF_8))

  }

}
