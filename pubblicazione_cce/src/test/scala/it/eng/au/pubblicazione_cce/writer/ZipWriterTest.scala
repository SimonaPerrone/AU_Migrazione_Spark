package it.eng.au.pubblicazione_cce.writer

import it.eng.au.pubblicazione_cce.EnvironmentSparkTest
import it.eng.au.pubblicazione_cce.file.writer.FileWriter
import it.eng.au.pubblicazione_cce.mock.writer.ZipWriterLocalMock
import it.eng.au.pubblicazione_cce.model.file.{FileModel, ZipFileModel}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.junit.{Assert, Before}

import java.io.File

class ZipWriterTest extends EnvironmentSparkTest {

  val spark = Environment.getSpark

  import spark.implicits._

  val userDir: String = System.getProperty("user.dir")
  val filePathRoot: String = s"${userDir}/target/test/"
  val filePathSubDirectoriesCsv: String = s"writer/ZipWriter/csv/"
  // dati csv
  val fileName1: String = "test_file1.csv"
  val fileName2: String = "test_file2.csv"
  val fileFullName1: String = filePathRoot + filePathSubDirectoriesCsv + fileName1
  val fileFullName2: String = filePathRoot + filePathSubDirectoriesCsv + fileName2
  val fileContent: String = "a,b,c\n1,2,3"

  // Scrive CSV da comprimere in file zip
  def writeCsv(): Unit = {
    val fileModel = Seq(
      FileModel(
        filePathRoot = filePathRoot,
        filePathSubDirectories = Some(filePathSubDirectoriesCsv),
        fileName = fileName1,
        fileFullName = fileFullName1,
        fileContent = fileContent.getBytes
      ),
      FileModel(
        filePathRoot = filePathRoot,
        filePathSubDirectories = Some(filePathSubDirectoriesCsv),
        fileName = fileName2,
        fileFullName = fileFullName2,
        fileContent = fileContent.getBytes
      )
    ).toDS()

    // scrivi file csv
    new FileWriter().write(fileModel)
  }

  // Scrive 1 file zip con i due file CSV all interno
  def testWrite(): Unit = {
    writeCsv()

    val maxBytesSizeZip = 20000

    val filePathSubDirectoriesZip = s"writer/ZipWriter/zip/write/"

    // zip writer per comprimere i csv scritti in precedenza
    val zipWriter = new ZipWriterLocalMock(
      MAX_BYTES_SIZE_ZIP = maxBytesSizeZip,
      outputFilePath = filePathRoot,
      numberOfSubDirectories = 4
    )

    val zipFileName = "zipfile"

    val zipFile = Seq(
      ZipFileModel(
        id_richiesta = "1",
        files = List(fileFullName1, fileFullName2), // lista file (nome completo) da aggiungere allo zip
        filePathRoot = filePathRoot,
        fileName = zipFileName, // file name zip
        filePathSubDirectories = filePathSubDirectoriesZip)
    ).toDS

    val expectedZipFile = "zipfile_1.zip"
    val result = zipWriter.write(zipFile)


    Assert.assertEquals(expectedZipFile, result.head.zipFiles.head)
    Assert.assertEquals(1, result.head.nZipFiles)

    Assert.assertTrue(new File(filePathRoot + filePathSubDirectoriesZip + expectedZipFile).exists())
  }

  // Scrive due file zip in quanto il numero di byte massimi per zip viene superato
  def testWrite_maxSize(): Unit = {
    writeCsv()

    val maxBytesSizeZip = 1L // size 1Byte -> piu' zip in output attesi
    val filePathSubDirectoriesZip = s"writer/ZipWriter/zip/writeMaxSize/"

    // zip writer per comprimere i csv scritti in precedenza
    val zipWriter = new ZipWriterLocalMock(
      MAX_BYTES_SIZE_ZIP = maxBytesSizeZip,
      outputFilePath = filePathRoot,
      numberOfSubDirectories = 4
    )

    val zipFileName = "zipfile_maxSize"

    val zipFile = Seq(
      ZipFileModel(
        id_richiesta = "1",
        files = List(fileFullName1, fileFullName2), // lista file (nome completo) da aggiungere allo zip
        filePathRoot = filePathRoot,
        fileName = zipFileName, // file name zip
        filePathSubDirectories = filePathSubDirectoriesZip)
    ).toDS

    val expectedZipFile1 = "zipfile_maxSize_1.zip"
    val expectedZipFile2 = "zipfile_maxSize_2.zip"
    val result = zipWriter.write(zipFile)


    Assert.assertEquals(2, result.head.zipFiles.length)
    Assert.assertEquals(2, result.head.nZipFiles)

    Assert.assertTrue(new File(filePathRoot + filePathSubDirectoriesZip + expectedZipFile1).exists())
    Assert.assertTrue(new File(filePathRoot + filePathSubDirectoriesZip + expectedZipFile2).exists())
  }

}

