package it.sferanet.au.controller.ca.forzature

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.controller.ca.forzature.ForcingControllerTest._
import it.sferanet.au.controller.coeffCorr.MockFlow
import it.sferanet.au.model.{Flow, MeasureValueType}
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.junit.Assert

class ForcingControllerTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()
  Environment.setProperty("z.sup.date", "2010-05-31")
  Environment.setProperty("z.inf.date", "2008-01-02")

  def testOnePdrWithOneIm1ForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau_int_cor = List(Some(3), Some(5))
    val date = "2008/06/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) ::: MockFlow.generateIm1(pdr1, date, cau_int_cor = cor)
      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect().sortBy(_.date)
      //      res.foreach(f => println(f.forcing + " "+f))

      if (cor == Some(3)) {
        res.foreach(f => {
          val forcingCode = ForcingController.getForcingCode(f.forcing)
          Assert.assertTrue(forcingCode.isDefined)
          if (formatter.format(f.date.get) < date) {
            Assert.assertEquals("IM1PRE3", f.forcing.get)
            Assert.assertEquals(MeasureValueType.C, forcingCode.get)
          } else {
            Assert.assertEquals("IM1POST3", f.forcing.get)
            Assert.assertEquals(MeasureValueType.K, forcingCode.get)
          }
        })
      }
      else {
        res.foreach(f => {
          val forcingCode = ForcingController.getForcingCode(f.forcing)
          Assert.assertTrue(forcingCode.isDefined)
          if (formatter.format(f.date.get) < date) {
            Assert.assertEquals("IM1PRE5", f.forcing.get)
            Assert.assertEquals(MeasureValueType.K, forcingCode.get)
          } else {
            Assert.assertEquals("IM1POST5", f.forcing.get)
            Assert.assertEquals(MeasureValueType.C, forcingCode.get)
          }
        })
      }

    }

  }

  def testOnePdrWithOneIm1NonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau_int_cor = List(None, Some(1), Some(2), Some(4))
    val date = "2008/06/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) ::: MockFlow.generateIm1(pdr1, date, cau_int_cor = cor)
      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.foreach(f => f.forcing.isEmpty)

    }

  }

  def testOnePdrWithOneIgmgForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau_int_cor = List(Some(2), Some(3), Some(4))
    val date = "2008/06/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) ::: MockFlow.generateIgmg(pdr1, date, cau_int_cor = cor)
      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      if (cor == Some(2)) {
        res.foreach(f => {
          val forcingCode = ForcingController.getForcingCode(f.forcing)
          Assert.assertTrue(forcingCode.isDefined)
          if (formatter.format(f.date.get) < date) {
            Assert.assertEquals("IGMGPRE2", f.forcing.get)
            Assert.assertEquals(MeasureValueType.C, forcingCode.get)
          } else {
            Assert.assertEquals("IGMGPOST2", f.forcing.get)
            Assert.assertEquals(MeasureValueType.K, forcingCode.get)
          }
        })
      }
      else {
        res.foreach(f => {
          val forcingCode = ForcingController.getForcingCode(f.forcing)
          Assert.assertTrue(forcingCode.isDefined)
          if (formatter.format(f.date.get) < date) {
            Assert.assertEquals("IGMGPRE" + cor.get, f.forcing.get)
            Assert.assertEquals(MeasureValueType.K, forcingCode.get)
          } else {
            Assert.assertEquals("IGMGPOST" + cor.get, f.forcing.get)
            Assert.assertEquals(MeasureValueType.C, forcingCode.get)
          }
        })
      }

    }

  }

  def testOnePdrWithOneIgmgNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau_int_cor = List(None, Some(1))
    val date = "2008/06/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) ::: MockFlow.generateIgmg(pdr1, date, cau_int_cor = cor)
      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.foreach(f => f.forcing.isEmpty)

    }

  }

  def testOnePdrWithTwoIm1ForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau_int_cor = List((Some(3), Some(3)), (Some(3), Some(5)), (Some(5), Some(3)), (Some(5), Some(5)))
    val date1 = "2008/04/02"
    val date2 = "2008/07/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) :::
        MockFlow.generateIm1(pdr1, date1, cau_int_cor = cor._1) :::
        MockFlow.generateIm1(pdr1, date2, cau_int_cor = cor._2)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      val (pre: MeasureValueType.Value, mid: MeasureValueType.Value, post: MeasureValueType.Value) = cor match {
        case (Some(3), Some(3)) => (MeasureValueType.C, MeasureValueType.C, MeasureValueType.K)
        case (Some(3), Some(5)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (Some(5), Some(5)) => (MeasureValueType.K, MeasureValueType.K, MeasureValueType.C)
        case (Some(5), Some(3)) => (MeasureValueType.K, MeasureValueType.C, MeasureValueType.K)
      }


      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        Assert.assertTrue(forcingCode.isDefined)
        val date = formatter.format(f.date.get)
        if (date < date1) {
          Assert.assertEquals("IM1PRE" + cor._1.get, f.forcing.get)
          Assert.assertEquals(pre, forcingCode.get)
        } else if (date < date2) {
          Assert.assertEquals("IM1PRE" + cor._2.get, f.forcing.get)
          Assert.assertEquals(mid, forcingCode.get)
        } else {
          Assert.assertEquals("IM1POST" + cor._2.get, f.forcing.get)
          Assert.assertEquals(post, forcingCode.get)
        }
      })
    }
  }

  def testOnePdrWithTwoIm1NonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau1 = List(None, Some(1), Some(2), Some(4))
    val cau2 = List(None, Some(1), Some(2), Some(4))
    val cau_int_cor = (for (i <- cau1; j <- cau2) yield (i, j))
    val date1 = "2008/04/02"
    val date2 = "2008/07/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) :::
        MockFlow.generateIm1(pdr1, date1, cau_int_cor = cor._1) :::
        MockFlow.generateIm1(pdr1, date2, cau_int_cor = cor._2)
      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.foreach(f => f.forcing.isEmpty)

    }

  }

  def testOnePdrWithTwoIgmgForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau = List(Some(2), Some(3), Some(4))
    val cau_int_cor = (for (i <- cau; j <- cau) yield (i, j))
    val date1 = "2008/04/02"
    val date2 = "2008/07/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) :::
        MockFlow.generateIgmg(pdr1, date1, cau_int_cor = cor._1) :::
        MockFlow.generateIgmg(pdr1, date2, cau_int_cor = cor._2)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      val (pre: MeasureValueType.Value, mid: MeasureValueType.Value, post: MeasureValueType.Value) = cor match {
        case (Some(2), Some(2)) => (MeasureValueType.C, MeasureValueType.C, MeasureValueType.K)
        case (Some(2), Some(4)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (Some(2), Some(3)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (_, Some(2)) => (MeasureValueType.K, MeasureValueType.C, MeasureValueType.K)
        case (_, _) => (MeasureValueType.K, MeasureValueType.K, MeasureValueType.C)
      }


      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        Assert.assertTrue(forcingCode.isDefined)
        val date = formatter.format(f.date.get)
        if (date < date1) {
          Assert.assertEquals("IGMGPRE" + cor._1.get, f.forcing.get)
          Assert.assertEquals(pre, forcingCode.get)
        } else if (date < date2) {
          Assert.assertEquals("IGMGPRE" + cor._2.get, f.forcing.get)
          Assert.assertEquals(mid, forcingCode.get)
        } else {
          Assert.assertEquals("IGMGPOST" + cor._2.get, f.forcing.get)
          Assert.assertEquals(post, forcingCode.get)
        }
      })


    }

  }

  def testOnePdrWithTwoIgmgNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau = List(None, Some(1))
    val cau_int_cor = (for (i <- cau; j <- cau) yield (i, j))
    val date1 = "2008/04/02"
    val date2 = "2008/07/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) :::
        MockFlow.generateIgmg(pdr1, date1, cau_int_cor = cor._1) :::
        MockFlow.generateIgmg(pdr1, date2, cau_int_cor = cor._2)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.foreach(f => f.forcing.isEmpty)

    }

  }

  def testOnePdrWithTwoIgmgForcingNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val cau1 = List(Some(2), Some(3), Some(4))
    val cau2 = List(None, Some(1))
    val cau_int_cor = (for (i <- cau1; j <- cau2) yield (i, j))
    val date1 = "2008/04/02"
    val date2 = "2008/07/02"

    for (cor <- cau_int_cor) {
      val measures = listMockFlowsOnePdr.map(_.asInstanceOf[Flow]) :::
        MockFlow.generateIgmg(pdr1, date1, cau_int_cor = cor._1) :::
        MockFlow.generateIgmg(pdr1, date2, cau_int_cor = cor._2)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      val (pre: Option[MeasureValueType.Value], mid: Option[MeasureValueType.Value], post: Option[MeasureValueType.Value]) =
        cor match {
          case (Some(2), _) => (Some(MeasureValueType.C), Some(MeasureValueType.K), Some(MeasureValueType.K))
          case (Some(3), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (Some(4), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (_, Some(2)) => (None, Some(MeasureValueType.C), Some(MeasureValueType.K))
          case (_, _) => (None, Some(MeasureValueType.K), Some(MeasureValueType.C))
        }

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)

        val date = formatter.format(f.date.get)
        if (date < date1) {
          Assert.assertEquals("IGMGPRE" + cor._1.get, f.forcing.get)
          Assert.assertEquals(pre, forcingCode)
        } else if (date < date2) {
          Assert.assertEquals("IGMGPOST" + cor._1.get, f.forcing.get)
          Assert.assertEquals(mid, forcingCode)
        } else {
          Assert.assertTrue(f.forcing.isEmpty)
        }
      })

    }

  }


  def testScenario1(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/07/14", "2008/11/14", "2009/03/14")
    val cm1: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm3: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4: List[CMFlow] = List(CMFlow("IGMG", Some(3)), CMFlow("IGMG", Some(4)), CMFlow("IM1", Some(5)))

    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) ::: getMockFlow(cmflow4, dates(3), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        Assert.assertTrue(forcingCode.isDefined)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        }
        else if (d < dates(1)) {
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        } else if (d < dates(2)) {
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.K, forcingCode.get)
        } else if (d < dates(3)) {
          Assert.assertEquals(cmflow4.service + "PRE" + cmflow4.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.K, forcingCode.get)
        } else if (d > dates(3)) {
          Assert.assertEquals(cmflow4.service + "POST" + cmflow4.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        }
      })
    }

  }

  def testScenario2(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/07/14", "2008/11/14", "2009/03/14")
    val cm1: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm3: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)))
    val cm4: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)))

    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) ::: getMockFlow(cmflow4, dates(3), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      //      controller.getMap(measuresRDD).collect().foreach(println)
      ////      res.foreach(println)

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        }
        else if (d < dates(1)) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        } else if (d < dates(2)) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.K, forcingCode.get)
        }
        else {
          Assert.assertTrue(f.forcing.isEmpty)
          Assert.assertTrue(forcingCode.isEmpty)
        }
      })

    }
  }

  def testScenario3(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/07/14", "2008/11/14", "2009/03/14")
    val cm1: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(2)))
    val cm3: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))

    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) ::: getMockFlow(cmflow4, dates(3), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      //      controller.getMap(measuresRDD).collect().foreach(println)
      //      res.foreach(println)

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        }
        else if (d < dates(1)) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.C, forcingCode.get)
        }
        else if (d < dates(2)) {
          Assert.assertTrue(forcingCode.isDefined)
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(MeasureValueType.K, forcingCode.get)
        } else {
          Assert.assertTrue(f.forcing.isEmpty)
          Assert.assertTrue(forcingCode.isEmpty)
        }
      })

    }
  }

  def testIm15Igmg4Consec(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/03/15")
    val cm1: List[CMFlow] = List(CMFlow("IM1", Some(5)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(4)))


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      controller.getMap(measuresRDD).collect().sortBy(f => {
        f._1._2
      }).foreach(f => {
        println(f._2, f._1)
      })
      res.sortBy(_.date).foreach(f => {
        println(f.forcing, f)
      })

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.K), forcingCode)
        }
        else if (d < dates(1)) {
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.K), forcingCode)
        } else {
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        }
      })

    }
  }

  def testIm13Igmg4Consec(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/03/15")
    val cm1: List[CMFlow] = List(CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(4)))


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      controller.getMap(measuresRDD).collect().sortBy(f => {
        f._1._2
      }).foreach(f => {
        println(f._2, f._1)
      })
      res.sortBy(_.date).foreach(f => {
        println(f.forcing, f)
      })

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        }
        else if (d < dates(1)) {
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.K), forcingCode)
        } else {
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        }
      })

    }
  }

  def testIm13twoIgmgNFConsec(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/12", "2008/05/15", "2008/05/16")
    val cm1: List[CMFlow] = List(CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IGMG", Some(1)))
    val cm3: List[CMFlow] = List(CMFlow("IGMG", Some(1)))


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1) ::: getMockFlow(cmflow3, dates(2), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      controller.getMap(measuresRDD).collect().sortBy(f => {
        f._1._2
      }).foreach(f => {
        println(f._2, f._1)
      })
      res.sortBy(_.date).foreach(f => {
        println(f.forcing, f)
      })

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        } else if (d < dates(1)) {
          Assert.assertEquals(cmflow1.service + "POST" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.K), forcingCode)
        }
        else {
          Assert.assertTrue(f.forcing.isEmpty)
          Assert.assertTrue(forcingCode.isEmpty)
        }
      })

    }
  }


  def testScenario4(): Unit = {
    Environment.setProperty("z.sup.date", "2010-05-31")
    Environment.setProperty("z.inf.date", "2008-01-02")
    val dates = List("2008/03/14", "2008/07/14", "2008/11/14", "2009/03/14")
    val cm1: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2: List[CMFlow] = List(CMFlow("IM1", Some(3)))
    val cm3: List[CMFlow] = List(CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))

    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val meas = measures ::: getMockFlow(cmflow1, dates.head, pdr1) ::: getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) ::: getMockFlow(cmflow4, dates(3), pdr1)

      val measuresRDD = Environment.getSparkContext.parallelize(meas.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      //      controller.getMap(measuresRDD).collect().foreach(println)
      //      res.foreach(println)

      res.foreach(f => {
        val forcingCode = ForcingController.getForcingCode(f.forcing)
        val d = formatter.format(f.date.get)
        if (d < dates.head) {
          Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        }
        else if (d < dates(1)) {
          Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.C), forcingCode)
        } else if (d < dates(2)) {
          Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.forcing.get)
          Assert.assertEquals(Some(MeasureValueType.K), forcingCode)
        } else {
          Assert.assertTrue(f.forcing.isEmpty)
          Assert.assertTrue(forcingCode.isEmpty)
        }
      })

    }
  }
}

object ForcingControllerTest {
  case class CMFlow2(pdr: String, date: String, cau_int_cor: Option[Int])

  case class CMFlow(service: String, cau_int_cor: Option[Int])

  def getMockFlow(cmflow1: CMFlow, date: String, pdr: String): List[Flow] = {
    if (cmflow1.service == "IM1")
      MockFlow.generateIm1(pdr, date, cau_int_cor = cmflow1.cau_int_cor)
    else
      MockFlow.generateIgmg(pdr, date, cau_int_cor = cmflow1.cau_int_cor)
  }

  val pdr1: String = "pdr1"
  val pdr2: String = "pdr2"

  val listBig = MockFlow.generateNMockFlows(17, List(pdr1, pdr2))
  val measures = listBig.filter(f => f.pdr == pdr1).map(_.asInstanceOf[Flow])

  lazy val listMockFlows = MockFlow.generateNMockFlows(11, List(pdr1, pdr2))
  lazy val listMockFlowsOnePdr = listMockFlows.filter(f => f.pdr == pdr1)

  Environment.setProperty("flow.read.startDate", "0")
  Environment.setProperty("flow.read.endDate", "202112")

  val controller = new ForcingController

  val formatter = Constants.getFormatter("yyyy/MM/dd")
}
