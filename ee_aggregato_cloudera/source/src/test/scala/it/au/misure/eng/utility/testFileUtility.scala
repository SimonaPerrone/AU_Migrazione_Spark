package it.au.misure.eng.utility

import java.io.{BufferedReader, File, FileReader}

import junit.framework.TestCase
import org.junit.Assert

class testFileUtility extends TestCase {

  def testWriteCSV(): Unit ={
    val outputPathFile = "src/test/resources/writeCsvTest/reportFileCsv.csv"
    val header = List("header1","header2")
    val row1 = List("value11;value12;","value21;value22;")
    val row2 = List("v11;v12;","v21;v22;")


    FileUtility.writeCsv(outputPathFile, header.mkString(";"), row1)
    FileUtility.writeCsv(outputPathFile, header.mkString(";"), row2, appendMode = true)

    val reportFile = new File(outputPathFile)

    Assert.assertFalse( reportFile.length()== 0 )
    val br = new BufferedReader( new FileReader(reportFile))

    Assert.assertTrue( br.readLine().equals( header.mkString(";") ))
    Assert.assertTrue( br.readLine().equals( row1.head ))
    Assert.assertTrue( br.readLine().equals( row1(1) ))
    Assert.assertTrue( br.readLine().equals( row2.head ))
    Assert.assertTrue( br.readLine().equals( row2(1) ))

  }
}
