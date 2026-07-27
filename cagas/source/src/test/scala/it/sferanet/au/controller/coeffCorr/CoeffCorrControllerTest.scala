package it.sferanet.au.controller.coeffCorr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.junit.Assert

import java.util.Date

class CoeffCorrControllerTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val pdr1: String = "pdr1"
  val pdr2: String = "pdr2"
  lazy val listMockFlows = MockFlow.generateNMockFlows(11, List(pdr1, pdr2))
  lazy val listMockFlowsOnePdr = listMockFlows.filter(f => f.pdr == pdr1)

  Environment.setProperty("flow.read.startDate", "0")
  Environment.setProperty("flow.read.endDate", "202112")

  val controller = new CoeffCorrController

  val formatter = Constants.getFormatter("yyyy/MM/dd")


  def checkMeasure(r: ((String, Date, String), Double), dateCM1: String,
                   dateCM2: String, coeff_pre1: Double, coeff_post1: Double,
                   coeff_pre2: Double, coeff_post2: Double): Boolean = {
    val res = {
      if (formatter.format(r._1._2) < dateCM1)
        r._2 == coeff_pre1
      else if (formatter.format(r._1._2) == dateCM1)
        if (r._1._3 == "IM1PRE" || r._1._3 == "IGMGPRE") r._2 == coeff_pre1
        else r._2 == coeff_pre2
      else if (formatter.format(r._1._2) < dateCM2)
        r._2 == coeff_pre2
      else if (formatter.format(r._1._2) == dateCM2)
        if (r._1._3 == "IM1PRE" || r._1._3 == "IGMGPRE") r._2 == coeff_pre2
        else r._2 == coeff_post2
      else
        r._2 == coeff_post2
    }
    if (res == false) println(r)
    res
  }

  def checkMeasure(r: ((String, Date, String), Double), dateCM1: String,
                   coeff_pre1: Double, coeff_post1: Double): Boolean = {
    if (formatter.format(r._1._2) < dateCM1)
      r._2 == coeff_pre1
    else if (formatter.format(r._1._2) == dateCM1)
      if (r._1._3 == "IM1PRE" || r._1._3 == "IGMGPRE") r._2 == coeff_pre1
      else r._2 == coeff_post1
    else
      r._2 == coeff_post1
  }

  def checkMeasureCoeff(r: ((String, Option[Date], String), Double), dateCM1: String,
                        dateCM2: String, coeff_pre1: Double, coeff_post1: Double,
                        coeff_pre2: Double, coeff_post2: Double): Boolean = {
    val rcopy = ((r._1._1, r._1._2.get, r._1._3), r._2)
    checkMeasure(rcopy, dateCM1, dateCM2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)
  }

  def checkMeasureCoeff(r: ((String, Option[Date], String), Double), dateCM1: String,
                        coeff_pre1: Double, coeff_post1: Double): Boolean = {
    val rcopy = ((r._1._1, r._1._2.get, r._1._3), r._2)
    checkMeasure(rcopy, dateCM1, coeff_pre1, coeff_post1)
  }

  def testOnePdrWithoutCMMeasures(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val measures = listMockFlowsOnePdr

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

    val res = controller.get(measuresRDD)

    Assert.assertTrue(res.isEmpty())
    res.collect.foreach(r => println(r._2))

    measuresRDD.map(m => ((m.pdr, m.date, m.service), m)) //
      .leftOuterJoin(res) // v._1 chiave (m.pdr,m.date,m.service)  v._2._1 flusso, v._2._2 coeff
      .map(v => v._2._1.changeCoeff(v._2._2)).collect().foreach(r => {
      print(r.coef);
      print("-");
      println(r)
    })

  }

  def testOnePdrWithOneIm1Beginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-02")

    val date = "2008/01/01"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))

  }

  def testOnePdrWithOneIm1Middle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date = "2008/05/01"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))
  }

  def testOnePdrWithOneIm1End(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date = "2008/12/30"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))

  }

  def testOnePdrWithOneIgmgBeginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date = "2008/01/01"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)

    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))
  }

  def testOnePdrWithOneIgmgMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date = "2008/05/01"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))
  }

  def testOnePdrWithOneIgmgEnd(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date = "2008/05/01"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date, coeff_pre1, coeff_post1)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date, coeff_pre1, coeff_post1)))

  }

  def testOnePdrWithTwoConsecIm1Beginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val date1 = "2008/01/01"
    val date2 = "2008/01/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoConsecIm1Middle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val deate1 = "2008/05/01"
    val date2 = "2008/05/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, deate1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, deate1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoConsecIm1End(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/12/29"
    val date2 = "2008/12/30"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoConsecIgmgBeginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val date1 = "2008/01/01"
    val date2 = "2008/01/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithTwoConsecIgmgMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val deate1 = "2008/05/01"
    val date2 = "2008/05/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, deate1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, deate1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoConsecIgmgEnd(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val deate1 = "2008/12/29"
    val date2 = "2008/12/30"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, deate1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, deate1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithTwoNotConsecIm1Beginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/01/01"
    val date2 = "2008/03/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoNotConsecIm1Middle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/03/01"
    val date2 = "2008/06/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoNotConsecIm1End(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/06/01"
    val date2 = "2008/12/25"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoNotConsecIgmgBeginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/01/01"
    val date2 = "2008/03/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoNotConsecIgmgMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val igmg1DateString = "2008/05/05"
    val igmg2DateString = "2008/08/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr :::
      MockFlow.generateIgmgMockFlow(pdr1, igmg1DateString, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, igmg2DateString, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, igmg1DateString, igmg2DateString, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithTwoNotConsecIgmgEnd(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val igmg1DateString = "2008/08/05"
    val igmg2DateString = "2008/12/21"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, igmg1DateString, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, igmg2DateString, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, igmg1DateString, igmg2DateString, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIm1IgmgBeginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val date1 = "2008/01/01"
    val date2 = "2008/01/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIm1IgmgMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/05"
    val date2 = "2008/05/08"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIm1IgmgEnd(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val date1 = "2008/12/21"
    val date2 = "2008/12/25"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithNotConsecIm1IgmgBeginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/01/01"
    val date2 = "2008/05/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithNotConsecIm1IgmgMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/01"
    val date2 = "2008/08/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithNotConsecIm1IgmgEnd(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/08/01"
    val date2 = "2008/12/25"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIgmgIm1Beginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val date1 = "2008/01/01"
    val date2 = "2008/01/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIgmgIm1Middle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/01"
    val date2 = "2008/05/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))

  }

  def testOnePdrWithConsecIgmgIm1End(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/12/21"
    val date2 = "2008/12/25"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithNotConsecIgmgIm1Beginning(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/01/01"
    val date2 = "2008/05/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithNotConsecIgmgIm1Middle(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/01"
    val date2 = "2008/08/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testOnePdrWithNotConsecIgmgIm1End(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/01"
    val date2 = "2008/12/25"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val measures = listMockFlowsOnePdr ::: MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD)
    Assert.assertFalse(res.isEmpty())
    res.collect.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))


  }

  def testTwoPdrsWithTwoMiddleIm1(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/05/01"
    val date2 = "2008/07/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val date3 = "2008/03/01"
    val date4 = "2008/04/05"

    val coeff_pre3 = 0.5
    val coeff_post3 = 0.6
    val coeff_pre4 = 0.7
    val coeff_post4 = 0.8

    val measures = listMockFlows :::
      MockFlow.generateIm1MockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2) :::
      MockFlow.generateIm1MockFlow(pdr2, date3, coeff_pre3, coeff_post3) :::
      MockFlow.generateIm1MockFlow(pdr2, date4, coeff_pre4, coeff_post4)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD).collect()
    Assert.assertFalse(res.isEmpty)
    val res_pdr1 = res.filter(_._1._1 == pdr1)
    val res_pdr2 = res.filter(_._1._1 == pdr2)

    res_pdr1.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))
    res_pdr2.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date3, date4, coeff_pre3, coeff_post3, coeff_pre4, coeff_post4)))


  }

  def testTwoPdrsWithTwoMiddleIgmg(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/03/01"
    val date2 = "2008/07/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val date3 = "2008/04/01"
    val date4 = "2008/04/05"

    val coeff_pre3 = 0.5
    val coeff_post3 = 0.6
    val coeff_pre4 = 0.7
    val coeff_post4 = 0.8

    val measures = listMockFlows :::
      MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIgmgMockFlow(pdr1, date2, coeff_pre2, coeff_post2) :::
      MockFlow.generateIgmgMockFlow(pdr2, date3, coeff_pre3, coeff_post3) :::
      MockFlow.generateIgmgMockFlow(pdr2, date4, coeff_pre4, coeff_post4)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD).collect()
    Assert.assertFalse(res.isEmpty)
    val res_pdr1 = res.filter(_._1._1 == pdr1)
    val res_pdr2 = res.filter(_._1._1 == pdr2)

    res_pdr1.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))
    res_pdr2.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date3, date4, coeff_pre3, coeff_post3, coeff_pre4, coeff_post4)))


  }

  def testTwoPdrsWithMiddleIm1Igmg(): Unit = {
    Environment.setProperty("z.sup.date", "2030-05-01")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val date1 = "2008/04/01"
    val date2 = "2008/08/05"

    val coeff_pre1 = 0.1
    val coeff_post1 = 0.2
    val coeff_pre2 = 0.3
    val coeff_post2 = 0.4

    val date3 = "2008/03/01"
    val date4 = "2008/05/05"

    val coeff_pre3 = 0.5
    val coeff_post3 = 0.6
    val coeff_pre4 = 0.7
    val coeff_post4 = 0.8

    val measures = listMockFlows :::
      MockFlow.generateIgmgMockFlow(pdr1, date1, coeff_pre1, coeff_post1) :::
      MockFlow.generateIm1MockFlow(pdr1, date2, coeff_pre2, coeff_post2) :::
      MockFlow.generateIgmgMockFlow(pdr2, date3, coeff_pre3, coeff_post3) :::
      MockFlow.generateIm1MockFlow(pdr2, date4, coeff_pre4, coeff_post4)

    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))
    val res = controller.get(measuresRDD).collect()
    Assert.assertFalse(res.isEmpty)
    val res_pdr1 = res.filter(_._1._1 == pdr1)
    val res_pdr2 = res.filter(_._1._1 == pdr2)

    res_pdr1.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date1, date2, coeff_pre1, coeff_post1, coeff_pre2, coeff_post2)))
    res_pdr2.foreach(r => Assert.assertTrue(checkMeasureCoeff(r, date3, date4, coeff_pre3, coeff_post3, coeff_pre4, coeff_post4)))

  }

}
