package it.au.misure.calcolo_capacita.component.implementation

import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.junit.Assert

class CalculationRangeTest extends ForUnitTest with Checker {

  test("test Calculation.calculateRange") {

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    val y = 100
    val (left, right) = Calculation.calculateRange(dataCalc, y)
    println(left.replaceAll("-","").substring(0,6))
    Assert.assertTrue(left == "2020-11-21")
    Assert.assertTrue(right == "2021-02-28")

  }

  test("test Calculation.calculateRangeDb") {

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    val y = 100
    val toReturn =Calculation.calculateRangeDb(dataCalc, y)

    //2021-03-08 => 2021-02-28 - 100 giorni = 20 Novembre 2020
    Assert.assertTrue(toReturn.toList === List("202011", "202012", "202101","202102"))


    val dataCalc2= "2021/11/03".getLocalDate("yyyy/MM/dd")
    val y2 = 365
    val toReturn2 =Calculation.calculateRangeDb(dataCalc2, y2)


    //2021-03-08 => 2021-02-28 - 365 giorni = 31 Ottobre 2020
    Assert.assertTrue(toReturn2.toList === List("202011", "202012", "202101","202102","202103","202104","202105","202106","202107","202108","202109","202110"))


    val dataCalc3= "2021/11/03".getLocalDate("yyyy/MM/dd")
    val y3 = 30
    val toReturn3 =Calculation.calculateRangeDb(dataCalc3, y3)

    //2021-11-03 =>  2021-02-28 - 30 giorni = 1 Ottobre 2021
    Assert.assertTrue(toReturn3.toList === List("202110"))

    val dataCalc4= "2022/01/03".getLocalDate("yyyy/MM/dd")
    val y4 = 60
    val toReturn4 =Calculation.calculateRangeDb(dataCalc4, y4)

    //2022-01-03 => 2021-12-31 - 60 giorni = 1 Novembre 2021
    Assert.assertTrue(toReturn4.toList === List("202111","202112"))

    val dataCalc5= "2022/01/03".getLocalDate("yyyy/MM/dd")
    val y5 = 33
    val toReturn5 =Calculation.calculateRangeDb(dataCalc5, y5)

    print(toReturn5)
    //2022-01-03=> 2021-12-31 - 33 giorni = 28 Novembre 2021
    Assert.assertTrue(toReturn5.toList === List("202111","202112"))

    val dataCalc6= "2022/01/03".getLocalDate("yyyy/MM/dd")
    val y6 = 730
    val toReturn6 =Calculation.calculateRangeDb(dataCalc6, y6)

    //2022-01-03=> 2021-12-31 - 730 giorni = 1 Gennaio 2020
    Assert.assertTrue(toReturn6.toList === List("202001","202002","202003","202004","202005","202006","202007","202008","202009","202010","202011","202012",
    "202101","202102","202103","202104","202105","202106","202107","202108","202109","202110","202111","202112"))



  }


}
