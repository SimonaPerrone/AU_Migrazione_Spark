package it.au.misure.calcolo_capacita.component.utility.`object`

import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate.ConvertString
import it.au.misure.calcolo_capacita.utility.ForUnitTest
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.junit.Assert


class RangeTest extends ForUnitTest{

  val formatDate=(date:String)=>{LocalDate.parse(date,DateTimeFormat.forPattern("yyyy-MM-dd"))}

  test("testCase1") {

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    val y=4
    val x=4
    val r=Range(dataCalc,y)

    Assert.assertTrue(r.rigth == formatDate("2021-02-28"))
    Assert.assertTrue(r.left == formatDate("2021-02-25"))

  }

  test("testCase2") {

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    val y=40

    val r=Range(dataCalc,y)
    Assert.assertTrue(r.rigth == formatDate("2021-02-28"))
    Assert.assertTrue(r.left == formatDate("2021-01-20"))

  }
}
