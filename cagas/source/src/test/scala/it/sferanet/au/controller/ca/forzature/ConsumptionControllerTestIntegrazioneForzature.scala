package it.sferanet.au.controller.ca.forzature


import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.controller.ca.ConsumptionController
//import it.sferanet.au.controller.ca.forzature.ConsumptionControllerTestIntegrazioneForzature._
import it.sferanet.au.controller.coeffCorr.MockFlow
import it.sferanet.au.model._
import it.sferanet.au.model.periodico.{Tgl, Tml}
import it.sferanet.au.model.prestazionale.{Im1Post, Im1Pre}
import it.sferanet.au.model.rettifica.{Rgl, Rml}
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.junit.Assert

import scala.util.Random

class ConsumptionControllerTestIntegrazioneForzature extends EnvironmentSparkTest {/*

  val shortTests: Boolean = true
  val controller = new ForcingController
  Environment.setProperty("z.sup.date", "2020-05-31")
  Environment.setProperty("z.inf.date", "2008-01-11")

  def testOneIm1ForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val cau_int_cor = List(Some(3), Some(5))
    val date = "2008-06-06"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk

    for (cor <- cau_int_cor) {
      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIm1(pdr1, date, cor) :::
        generateIm1(pdr2, date, cor, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel ::: conv ::: pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions2.foreach(println)

        if (cor == Some(3)) {
          consumptions.foreach(f => {
            if (format.format(f.startSegment) < date) {
              Assert.assertEquals("IM1PRE3", f.tipoForzatura.get)
              checkConsumption(f, 100, 100)
            } else {
              Assert.assertEquals("IM1POST3", f.tipoForzatura.get)
              checkConsumption(f, 2, 2)
            }
          })
          consumptions2.foreach(f => {
            checkConsumption(f, 2, 2)
          })
        }
        else {
          consumptions.foreach(f => {
            if (format.format(f.startSegment) < date) {
              Assert.assertEquals("IM1PRE5", f.tipoForzatura.get)
              checkConsumption(f, 2, 2)
            } else {
              Assert.assertEquals("IM1POST5", f.tipoForzatura.get)
              checkConsumption(f, 100, 100)
            }
          })
          consumptions2.foreach(f => {
            checkConsumption(f, 2, 2)
          })
        }
      }
    }
  }


  def testOnePdrWithOneIm1NonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val cau_int_cor = List(None, Some(1), Some(2), Some(4))
    val date = "2008-06-06"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIm1(pdr1, date, cor) :::
        generateIm1(pdr2, date, cor, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 1, 1)
        })
      }

      for (misint_preconv <- conv) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        //        val consumptions2 = result.flatMap(_._2._1)
        //          .filter(_.pdr == pdr2)
        //          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 100, 100)
        })
        //        consumptions2.foreach(f => {checkConsumption(f, 2, 2)})
      }

      for (misint_preconv <- pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 2, 2)
        })

      }
    }

  }

  def testOnePdrWithOneIgmgForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cau_int_cor = List(Some(2), Some(3), Some(4))
    val date = "2008-06-06"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date, cor) :::
        generateIgmg(pdr2, date, cor, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel ::: conv ::: pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        if (cor == Some(2)) {
          consumptions.foreach(f => {
            if (format.format(f.startSegment) < date) {
              Assert.assertEquals("IGMGPRE2", f.tipoForzatura.get)
              checkConsumption(f, 100, 100)
            } else {
              Assert.assertEquals("IGMGPOST2", f.tipoForzatura.get)
              checkConsumption(f, 2, 2)
            }
          })
          consumptions2.foreach(f => {
            checkConsumption(f, 2, 2)
          })
        }
        else {
          consumptions.foreach(f => {
            if (format.format(f.startSegment) < date) {
              Assert.assertEquals("IGMGPRE" + cor.get, f.tipoForzatura.get)
              checkConsumption(f, 2, 2)
            } else {
              Assert.assertEquals("IGMGPOST" + cor.get, f.tipoForzatura.get)
              checkConsumption(f, 100, 100)
            }
          })
          consumptions2.foreach(f => {
            checkConsumption(f, 2, 2)
          })
        }
      }
    }

  }

  def testOnePdrWithOneIgmgNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val cau_int_cor = List(None, Some(1))
    val date = "2008-06-06"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date, cor)
      generateIgmg(pdr2, date, cor, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 1, 1)
        })

      }

      for (misint_preconv <- conv) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        //        val consumptions2 = result.flatMap(_._2._1)
        //          .filter(_.pdr == pdr2)
        //          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 100, 100)
        })
        //        consumptions2.foreach(f => {
        //          val startDate = format.format(f.startSegment)
        //          val endDate = format.format(f.endSegment)
        //          if(startDate!= date && endDate != date)
        //            checkConsumption(f, 2, 2)
        //        })
      }

      for (misint_preconv <- pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (startDate != date && endDate != date)
            checkConsumption(f, 2, 2)
        })
      }
    }
  }

  def testOnePdrWithTwoIm1ForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val cau_int_corList = List((Some(3), Some(3)), (Some(3), Some(5)), (Some(5), Some(3)), (Some(5), Some(5)))
    val cau_int_cor = if (shortTests) List(cau_int_corList(rnd.nextInt(cau_int_corList.length))) else cau_int_corList

    val date1 = "2008-05-05"
    val date2 = "2008-09-02"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIm1(pdr1, date1, cor._1) :::
        generateIm1(pdr1, date2, cor._2) :::
        generateIm1(pdr2, date1, cor._1, None) :::
        generateIm1(pdr2, date2, cor._2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))


      val (pre: MeasureValueType.Value, mid: MeasureValueType.Value, post: MeasureValueType.Value) = cor match {
        case (Some(3), Some(3)) => (MeasureValueType.C, MeasureValueType.C, MeasureValueType.K)
        case (Some(3), Some(5)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (Some(5), Some(5)) => (MeasureValueType.K, MeasureValueType.K, MeasureValueType.C)
        case (Some(5), Some(3)) => (MeasureValueType.K, MeasureValueType.C, MeasureValueType.K)
      }

      for (misint_preconv <- prel ::: conv ::: pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)


        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) "IM1PRE" + cor._1.get else if (date < date2) "IM1PRE" + cor._2.get else "IM1POST" + cor._2.get
          Assert.assertEquals(tipoForzatura, f.tipoForzatura.get)
          tipoMisura match {
            case MeasureValueType.C => checkConsumption(f, 100, 100)
            case MeasureValueType.K => checkConsumption(f, 2, 2)
            case _ => checkConsumption(f, 1, 1)
          }
        })
        consumptions2.foreach(f => {
          checkConsumption(f, 2, 2)
        })
      }

    }

  }

  def testOnePdrWithTwoIm1NonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cau1List = List(None, Some(1), Some(2), Some(4))
    val cau2List = List(None, Some(1), Some(2), Some(4))
    val cau1 = if (shortTests) List(cau1List(rnd.nextInt(cau1List.length))) else cau1List
    val cau2 = if (shortTests) List(cau2List(rnd.nextInt(cau2List.length))) else cau2List
    val cau_int_cor = (for (i <- cau1; j <- cau2) yield (i, j))
    val date1 = "2008-05-05"
    val date2 = "2008-09-02"

    val dates = date1 :: List(date2)

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIm1(pdr1, date1, cor._1) :::
        generateIm1(pdr1, date2, cor._2) :::
        generateIm1(pdr2, date1, cor._1, None) :::
        generateIm1(pdr2, date2, cor._2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(startDate) && !dates.contains(endDate))
            checkConsumption(f, 1, 1)
        })

      }

      for (misint_preconv <- conv) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        //      val consumptions2 = result.flatMap(_._2._1)
        //        .filter(_.pdr == pdr2)
        //        .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(startDate) && !dates.contains(endDate))
            checkConsumption(f, 100, 100)
        })
        //      consumptions2.foreach(f => {
        //        checkConsumption(f, 2, 2)
        //      })
      }

      for (misint_preconv <- pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val startDate = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(startDate) && !dates.contains(endDate))
            checkConsumption(f, 2, 2)
        })

      }
    }

  }

  def testOnePdrWithTwoIgmgForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cauList = List(Some(2), Some(3), Some(4))
    val cau = if (shortTests) List(cauList(rnd.nextInt(cauList.length))) else cauList
    val cau_int_cor = (for (i <- cau; j <- cau) yield (i, j))
    val date1 = "2008-05-05"
    val date2 = "2008-10-02"

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {
      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date1, cor._1) :::
        generateIgmg(pdr1, date2, cor._2) :::
        generateIgmg(pdr2, date1, cor._1, None) :::
        generateIgmg(pdr2, date2, cor._2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val (pre: MeasureValueType.Value, mid: MeasureValueType.Value, post: MeasureValueType.Value) = cor match {
        case (Some(2), Some(2)) => (MeasureValueType.C, MeasureValueType.C, MeasureValueType.K)
        case (Some(2), Some(4)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (Some(2), Some(3)) => (MeasureValueType.C, MeasureValueType.K, MeasureValueType.C)
        case (_, Some(2)) => (MeasureValueType.K, MeasureValueType.C, MeasureValueType.K)
        case (_, _) => (MeasureValueType.K, MeasureValueType.K, MeasureValueType.C)
      }


      for (misint_preconv <- prel ::: conv ::: pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) "IGMGPRE" + cor._1.get else if (date < date2) "IGMGPRE" + cor._2.get else "IGMGPOST" + cor._2.get
          Assert.assertEquals(tipoForzatura, f.tipoForzatura.get)
          tipoMisura match {
            case MeasureValueType.C => checkConsumption(f, 100, 100)
            case MeasureValueType.K => checkConsumption(f, 2, 2)
            case _ => checkConsumption(f, 1, 1)
          }
        })
        consumptions2.foreach(f => {
          checkConsumption(f, 2, 2)
        })
      }
    }
  }

  def testOnePdrWithTwoIgmgNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cauList = List(None, Some(1))
    val cau = if (shortTests) List(cauList(rnd.nextInt(cauList.length))) else cauList
    val cau_int_cor = (for (i <- cau; j <- cau) yield (i, j))
    val date1 = "2008-05-05"
    val date2 = "2008-09-02"
    val dates = date1 :: List(date2)

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date1, cor._1) :::
        generateIgmg(pdr1, date2, cor._2) :::
        generateIgmg(pdr2, date1, cor._1, None) :::
        generateIgmg(pdr2, date2, cor._2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 1, 1)
        })
      }

      for (misint_preconv <- conv) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        //          val consumptions2 = result.flatMap(_._2._1)
        //            .filter(_.pdr == pdr2)
        //            .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 100, 100)
        })
        //NOn controllo il consumption2 perchè la regola dice di prendere la convertita
        // è in contrasto con la misura. Questa cosa NON GESTITA dalla vecchia versione non
        // era parte dell'intervento
        //          consumptions2.foreach(f => {
        //            checkConsumption(f, 2, 2)
        //          })
      }

      for (misint_preconv <- pk) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)
        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          Assert.assertTrue(f.tipoForzatura.isEmpty)
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 2, 2)
        })

      }
    }
  }

  def testOnePdrWithTwoIgmgForcingNonForcingMiddle(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cau1List = List(Some(2), Some(3), Some(4))
    val cau2List = List(None, Some(1))
    val cau1 = if (shortTests) List(cau1List(rnd.nextInt(cau1List.length))) else cau1List
    val cau2 = if (shortTests) List(cau2List(rnd.nextInt(cau2List.length))) else cau2List
    val cau_int_cor = (for (i <- cau1; j <- cau2) yield (i, j))
    val date1 = "2008-05-05"
    val date2 = "2008-09-02"
    val dates = date1 :: List(date2)

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {

      val measures = measuresList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date1, cor._1) :::
        generateIgmg(pdr1, date2, cor._2) :::
        generateIgmg(pdr2, date1, cor._1, None) :::
        generateIgmg(pdr2, date2, cor._2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))


      val (pre: Option[MeasureValueType.Value], mid: Option[MeasureValueType.Value], post: Option[MeasureValueType.Value]) =
        cor match {
          case (Some(2), _) => (Some(MeasureValueType.C), Some(MeasureValueType.K), None)
          case (Some(3), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (Some(4), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (_, Some(2)) => (None, Some(MeasureValueType.C), Some(MeasureValueType.K))
          case (_, _) => (None, Some(MeasureValueType.K), Some(MeasureValueType.C))
        }

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)


        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date > date1 && date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case Some(MeasureValueType.K) => checkConsumption(f, 2, 2)
            case _ => {
              if (!dates.contains(date) && !dates.contains(endDate))
                checkConsumption(f, 1, 1)
            }
          }
        })
        consumptions2.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 2, 2)
            case Some(MeasureValueType.K) => checkConsumption(f, 2, 2)
            case _ => {
              if (!dates.contains(date) && !dates.contains(endDate))
                checkConsumption(f, 1, 1)
            }
          }
        })
      }

      for (misint_preconv <- conv) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)

          val tipoMisura = if (date < date1) pre else if (date > date1 && date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case Some(MeasureValueType.K) => checkConsumption(f, 2, 2)
            case _ => {
              if (!dates.contains(date) && !dates.contains(endDate))
                checkConsumption(f, 100, 100)
            }
          }
        })
        consumptions2.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date > date1 && date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(f.tipoForzatura, tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 2, 2)
            case Some(MeasureValueType.K) => checkConsumption(f, 2, 2)
            case _ => {}
          }
        })
      }

      for (misint_preconv <- pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)

          val tipoMisura = if (date < date1) pre else if (date > date1 && date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(f.tipoForzatura, tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case _ => {
              if (!dates.contains(date) && !dates.contains(endDate))
                checkConsumption(f, 2, 2)
            }
          }
        })
        consumptions2.foreach(f => {
          checkConsumption(f, 2, 2)
        })

      }
    }
  }

  def testOnePdrWithTwoIgmgForcingNonForcingMiddleWithRettifiche(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val cau1List = List(Some(2), Some(3), Some(4))
    val cau2List = List(None, Some(1))
    val cau1 = if (shortTests) List(cau1List(rnd.nextInt(cau1List.length))) else cau1List
    val cau2 = if (shortTests) List(cau2List(rnd.nextInt(cau2List.length))) else cau2List
    val cau_int_cor = (for (i <- cau1; j <- cau2) yield (i, j))
    val date1 = "2008-02-14"
    val date2 = "2008-03-14"

    val dates = date1 :: List(date2)

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (cor <- cau_int_cor) {


      val measures = rettificheList.map(_.asInstanceOf[Flow]) :::
        generateIgmg(pdr1, date1, cor._1) :::
        generateIgmg(pdr1, date2, cor._2)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))


      val (pre: Option[MeasureValueType.Value], mid: Option[MeasureValueType.Value], post: Option[MeasureValueType.Value]) =
        cor match {
          case (Some(2), _) => (Some(MeasureValueType.C), Some(MeasureValueType.K), None)
          case (Some(3), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (Some(4), _) => (Some(MeasureValueType.K), Some(MeasureValueType.C), None)
          case (_, Some(2)) => (None, Some(MeasureValueType.C), Some(MeasureValueType.K))
          case (_, _) => (None, Some(MeasureValueType.K), Some(MeasureValueType.C))
        }

      val forcingController = new ForcingController

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)


        consumptions.foreach(f => {
          //          println(f)
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case Some(MeasureValueType.K) =>
              if (!dates.contains(endDate) && !dates.contains(date))
                checkConsumption(f, 100, 100)
              else
                checkConsumption(f, 2, 2)
            case _ => checkConsumption(f, 1, 1)
          }
        })


      }

      for (misint_preconv <- conv) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case Some(MeasureValueType.K) =>
              if (!dates.contains(endDate) && !dates.contains(date))
                checkConsumption(f, 100, 100)
              else
                checkConsumption(f, 2, 2)
            case _ =>
              if (!dates.contains(endDate) && !dates.contains(date))
                checkConsumption(f, 100, 100)

          }
        })


      }

      for (misint_preconv <- pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          println(f)
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoMisura = if (date < date1) pre else if (date < date2) mid else post
          val tipoForzatura = if (date < date1) Some("IGMGPRE" + cor._1.get) else if (date < date2) Some("IGMGPOST" + cor._1.get) else None
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          //          if(!dates.contains(endDate) && !dates.contains(date))
          //            Assert.assertEquals(f.tipoForzatura, tipoForzatura)
          tipoMisura match {
            case Some(MeasureValueType.C) => checkConsumption(f, 100, 100)
            case Some(MeasureValueType.K) =>
              if (!dates.contains(endDate) && !dates.contains(date))
                checkConsumption(f, 100, 100)
              else
                checkConsumption(f, 2, 2)
            case _ => checkConsumption(f, 2, 2)
          }
        })

      }
    }
  }


  def testScenario1(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val dates = List("2008-03-14", "2008-07-14", "2008-11-14", "2009-03-14")
    val cm1List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm3List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4List: List[CMFlow] = List(CMFlow("IGMG", Some(3)), CMFlow("IGMG", Some(4)), CMFlow("IM1", Some(5)))

    val cm1 = if (shortTests) List(cm1List(rnd.nextInt(cm1List.length))) else cm1List
    val cm2 = if (shortTests) List(cm2List(rnd.nextInt(cm2List.length))) else cm2List
    val cm3 = if (shortTests) List(cm3List(rnd.nextInt(cm3List.length))) else cm3List
    val cm4 = if (shortTests) List(cm4List(rnd.nextInt(cm4List.length))) else cm4List

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val measures = measuresList :::
        getMockFlow(cmflow1, dates.head, pdr1) :::
        getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) :::
        getMockFlow(cmflow4, dates(3), pdr1) :::
        getMockFlow(cmflow1, dates.head, pdr2, None) :::
        getMockFlow(cmflow2, dates(1), pdr2, None) :::
        getMockFlow(cmflow3, dates(2), pdr2, None) :::
        getMockFlow(cmflow4, dates(3), pdr2, None)

      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel ::: conv ::: pk) {


        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        //        val rcuTechval = rcuTech(None,None)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val d = format.format(f.startSegment)
          if (d < dates.head) {
            Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          }
          else if (d < dates(1)) {
            Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          } else if (d < dates(2)) {
            Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 2, 2)
          }
          else if (d < dates(3)) {
            Assert.assertEquals(cmflow4.service + "PRE" + cmflow4.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 2, 2)
          } else if (d >= dates(3)) {
            Assert.assertEquals(cmflow4.service + "POST" + cmflow4.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          }

        })
        consumptions2.foreach(f => {
          checkConsumption(f, 2, 2)
        })
      }
    }

  }

  def testScenario2(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val dates = List("2008-03-14", "2008-07-14", "2008-11-14", "2009-03-14")
    val cm1List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm3List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)))
    val cm4List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)))

    val cm1 = if (shortTests) List(cm1List(rnd.nextInt(cm1List.length))) else cm1List
    val cm2 = if (shortTests) List(cm2List(rnd.nextInt(cm2List.length))) else cm2List
    val cm3 = if (shortTests) List(cm3List(rnd.nextInt(cm3List.length))) else cm3List
    val cm4 = if (shortTests) List(cm4List(rnd.nextInt(cm4List.length))) else cm4List

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val measures = measuresList :::
        getMockFlow(cmflow1, dates.head, pdr1) :::
        getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) :::
        getMockFlow(cmflow4, dates(3), pdr1) :::
        getMockFlow(cmflow1, dates.head, pdr2, None) :::
        getMockFlow(cmflow2, dates(1), pdr2, None) :::
        getMockFlow(cmflow3, dates(2), pdr2, None) :::
        getMockFlow(cmflow4, dates(3), pdr2, None)


      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel ::: conv ::: pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        //        val rcuTechval = rcuTech(None,None)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val d = format.format(f.startSegment)
          if (d < dates.head) {
            Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          }
          else if (d < dates(1)) {
            Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          }
          else if (d < dates(2)) {
            Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 2, 2)
          }
          else {
            Assert.assertTrue(f.tipoForzatura.isEmpty)
          }


        })
        consumptions2.foreach(f => {
          val d = format.format(f.startSegment)
          if (d < dates(2)) {
            checkConsumption(f, 2, 2)
          } else {
            Assert.assertTrue(f.tipoForzatura.isEmpty)
          }
        })
      }

    }
  }

  def testRmlRgl4o5Im1(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")
    val rettificheList = List(
      Rml(pdr = "1", service = "RML", motivation = Some(4), date = Some(format.parse("2008-01-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(5), date = Some(format.parse("2008-02-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(3), date = Some(format.parse("2008-03-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rml(pdr = "1", service = "RML", motivation = Some(5), date = Some(format.parse("2008-04-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(4), date = Some(format.parse("2008-05-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(2), date = Some(format.parse("2008-06-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(5), date = Some(format.parse("2008-07-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", service = "RGL", motivation = Some(5), date = Some(format.parse("2008-08-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )
    val im1List = List(
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-01-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-01-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-02-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-02-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-03-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-03-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-04-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-04-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-05-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-05-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-06-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-06-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some("rettificato"), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-07-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some(""), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-07-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = Some(""), readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "1", service = "IM1PRE", date = Some(format.parse("2008-08-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = None, readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "1", service = "IM1POST", date = Some(format.parse("2008-08-11")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, fileRettifica = None, readType = None, coefCorr = Some(1.0), cau_int_mis = Some(1), cau_int_cor = Some(5), pivaDistr = None, pivaUtente = None, ammissibilita = None)

    )

    val measures = rettificheList ::: im1List
    val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk

    for (misint_preconv <- prel ::: conv ::: pk) {

      val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
      //        val rcuTechval = rcuTech(None,None)
      val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcusVarProf)

      val consumptions = result.flatMap(_._2._1)
        .filter(_.pdr == pdr1)
        .collect.sortBy(_.startSegment.getTime)

      //consumo forzatura=IM1PRE5 tra RML(motivazione=4) e IM1PRE(Rettificato) --> CCC
      val c0 = consumptions.head
      Assert.assertEquals("RML", c0.startService)
      Assert.assertEquals("IM1PRE", c0.endService)
      Assert.assertEquals(Some("IM1PRE5"), c0.tipoForzatura)
      Assert.assertEquals("CCC", c0.coerenzaDim)
      Assert.assertEquals(100.0, c0.startvalue, 0)
      Assert.assertEquals(100.0, c0.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) e RGL(motivazione=5)  --> CCC
      val c1 = consumptions(1)
      Assert.assertEquals("IM1POST", c1.startService)
      Assert.assertEquals("RGL", c1.endService)
      Assert.assertEquals(Some("IM1PRE5"), c1.tipoForzatura)
      Assert.assertEquals("CCC", c1.coerenzaDim)
      Assert.assertEquals(100.0, c1.startvalue, 0)
      Assert.assertEquals(100.0, c1.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=5) e IM1PRE(Rettificato)  --> CCC
      val c2 = consumptions(2)
      Assert.assertEquals("RGL", c2.startService)
      Assert.assertEquals("IM1PRE", c2.endService)
      Assert.assertEquals(Some("IM1PRE5"), c2.tipoForzatura)
      Assert.assertEquals("CCC", c2.coerenzaDim)
      Assert.assertEquals(100.0, c2.startvalue, 0)
      Assert.assertEquals(100.0, c2.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) E RGL(motivazione=3) --> KKK
      val c3 = consumptions(3)
      Assert.assertEquals("IM1POST", c3.startService)
      Assert.assertEquals("RGL", c3.endService)
      Assert.assertEquals(Some("IM1PRE5"), c3.tipoForzatura)
      Assert.assertEquals("KKK", c3.coerenzaDim)
      Assert.assertEquals(1.0, c3.startvalue, 0)
      Assert.assertEquals(1.0, c3.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=3) IM1PRE(Rettificato) --> KKK
      val c4 = consumptions(4)
      Assert.assertEquals("RGL", c4.startService)
      Assert.assertEquals("IM1PRE", c4.endService)
      Assert.assertEquals(Some("IM1PRE5"), c4.tipoForzatura)
      Assert.assertEquals("KKK", c4.coerenzaDim)
      Assert.assertEquals(1.0, c4.startvalue, 0)
      Assert.assertEquals(1.0, c4.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) e RML(motivazione=5)  --> CCC
      val c5 = consumptions(5)
      Assert.assertEquals("IM1POST", c5.startService)
      Assert.assertEquals("RML", c5.endService)
      Assert.assertEquals(Some("IM1PRE5"), c5.tipoForzatura)
      Assert.assertEquals("CCC", c5.coerenzaDim)
      Assert.assertEquals(100.0, c5.startvalue, 0)
      Assert.assertEquals(100.0, c5.endvalue, 0)


      //consumo forzatura=IM1PRE5 tra RML(motivazione=5) IM1PRE(Rettificato) --> CCC
      val c6 = consumptions(6)
      Assert.assertEquals("RML", c6.startService)
      Assert.assertEquals("IM1PRE", c6.endService)
      Assert.assertEquals(Some("IM1PRE5"), c6.tipoForzatura)
      Assert.assertEquals("CCC", c6.coerenzaDim)
      Assert.assertEquals(100.0, c6.startvalue, 0)
      Assert.assertEquals(100.0, c6.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) e RGL(motivazione=4)  --> CCC
      val c7 = consumptions(7)
      Assert.assertEquals("IM1POST", c7.startService)
      Assert.assertEquals("RGL", c7.endService)
      Assert.assertEquals(Some("IM1PRE5"), c7.tipoForzatura)
      Assert.assertEquals("CCC", c7.coerenzaDim)
      Assert.assertEquals(100.0, c7.startvalue, 0)
      Assert.assertEquals(100.0, c7.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=4) IM1PRE(Rettificato) --> CCC
      val c8 = consumptions(8)
      Assert.assertEquals("RGL", c8.startService)
      Assert.assertEquals("IM1PRE", c8.endService)
      Assert.assertEquals(Some("IM1PRE5"), c8.tipoForzatura)
      Assert.assertEquals("CCC", c8.coerenzaDim)
      Assert.assertEquals(100.0, c8.startvalue, 0)
      Assert.assertEquals(100.0, c8.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) e RGL(motivazione=2)  --> KKK
      val c9 = consumptions(9)
      Assert.assertEquals("IM1POST", c9.startService)
      Assert.assertEquals("RGL", c9.endService)
      Assert.assertEquals(Some("IM1PRE5"), c9.tipoForzatura)
      Assert.assertEquals("KKK", c9.coerenzaDim)
      Assert.assertEquals(1.0, c9.startvalue, 0)
      Assert.assertEquals(1.0, c9.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=2) IM1PRE(Rettificato) --> KKK
      val c10 = consumptions(10)
      Assert.assertEquals("RGL", c10.startService)
      Assert.assertEquals("IM1PRE", c10.endService)
      Assert.assertEquals(Some("IM1PRE5"), c10.tipoForzatura)
      Assert.assertEquals("KKK", c10.coerenzaDim)
      Assert.assertEquals(1.0, c10.startvalue, 0)
      Assert.assertEquals(1.0, c10.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(Rettificato) e RGL(motivazione=5)  --> CCC
      val c11 = consumptions(11)
      Assert.assertEquals("IM1POST", c11.startService)
      Assert.assertEquals("RGL", c11.endService)
      Assert.assertEquals(Some("IM1PRE5"), c11.tipoForzatura)
      Assert.assertEquals("CCC", c11.coerenzaDim)
      Assert.assertEquals(100.0, c11.startvalue, 0)
      Assert.assertEquals(100.0, c11.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=5) IM1PRE(Non Rettificato) --> KKK
      val c12 = consumptions(12)
      Assert.assertEquals("RGL", c12.startService)
      Assert.assertEquals("IM1PRE", c12.endService)
      Assert.assertEquals(Some("IM1PRE5"), c12.tipoForzatura)
      Assert.assertEquals("KKK", c12.coerenzaDim)
      Assert.assertEquals(1.0, c12.startvalue, 0)
      Assert.assertEquals(1.0, c12.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra IM1POST(NON Rettificato) e RGL(motivazione=5)  --> KKK
      val c13 = consumptions(13)
      Assert.assertEquals("IM1POST", c13.startService)
      Assert.assertEquals("RGL", c13.endService)
      Assert.assertEquals(Some("IM1PRE5"), c13.tipoForzatura)
      Assert.assertEquals("KKK", c13.coerenzaDim)
      Assert.assertEquals(1.0, c13.startvalue, 0)
      Assert.assertEquals(1.0, c13.endvalue, 0)

      //consumo forzatura=IM1PRE5 tra RGL(motivazione=5) IM1PRE(Non Rettificato) --> KKK
      val c14 = consumptions(14)
      Assert.assertEquals("RGL", c14.startService)
      Assert.assertEquals("IM1PRE", c14.endService)
      Assert.assertEquals(Some("IM1PRE5"), c14.tipoForzatura)
      Assert.assertEquals("KKK", c14.coerenzaDim)
      Assert.assertEquals(1.0, c14.startvalue, 0)
      Assert.assertEquals(1.0, c14.endvalue, 0)

      consumptions.foreach(println)
    }
  }

  def testScenario3(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val dates = List("2008-03-14", "2008-07-14", "2008-11-14", "2009-03-14")
    val cm1List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2List: List[CMFlow] = List(CMFlow("IGMG", Some(2)))
    val cm3List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))

    val cm1 = if (shortTests) List(cm1List(rnd.nextInt(cm1List.length))) else cm1List
    val cm2 = if (shortTests) List(cm2List(rnd.nextInt(cm2List.length))) else cm2List
    val cm3 = if (shortTests) List(cm3List(rnd.nextInt(cm3List.length))) else cm3List
    val cm4 = if (shortTests) List(cm4List(rnd.nextInt(cm4List.length))) else cm4List

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk


    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val measures = measuresList :::
        getMockFlow(cmflow1, dates.head, pdr1) :::
        getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) :::
        getMockFlow(cmflow4, dates(3), pdr1) :::
        getMockFlow(cmflow1, dates.head, pdr2, None) :::
        getMockFlow(cmflow2, dates(1), pdr2, None) :::
        getMockFlow(cmflow3, dates(2), pdr2, None) :::
        getMockFlow(cmflow4, dates(3), pdr2, None)


      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      for (misint_preconv <- prel ::: conv ::: pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        //        val rcuTechval = rcuTech(None,None)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(f => {
          val d = format.format(f.startSegment)
          if (d < dates.head) {
            Assert.assertEquals(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          }
          else if (d < dates(1)) {
            Assert.assertEquals(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 100, 100)
          } else if (d < dates(2)) {
            Assert.assertEquals(cmflow2.service + "POST" + cmflow2.cau_int_cor.get, f.tipoForzatura.get)
            checkConsumption(f, 2, 2)
          } else {
            Assert.assertTrue(f.tipoForzatura.isEmpty)
          }
        })
        consumptions2.foreach(println)
        consumptions2.foreach(f => {
          val d = format.format(f.startSegment)
          if (d < dates(2)) {
            checkConsumption(f, 2, 2)
          } else {
            Assert.assertTrue(f.tipoForzatura.isEmpty)
          }
        })
      }

    }
  }

  def testScenario4(): Unit = {
    Environment.setProperty("z.sup.date", "2020-05-31")
    Environment.setProperty("z.inf.date", "2008-01-11")

    val dates = List("2008-03-14", "2008-07-14", "2008-10-14", "2009-01-14")
    val cm1List: List[CMFlow] = List(CMFlow("IGMG", Some(2)), CMFlow("IM1", Some(3)))
    val cm2List: List[CMFlow] = List(CMFlow("IM1", Some(3)))
    val cm3List: List[CMFlow] = List(CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))
    val cm4List: List[CMFlow] = List(CMFlow("IGMG", None), CMFlow("IGMG", Some(1)), CMFlow("IM1", None), CMFlow("IM1", Some(1)),
      CMFlow("IM1", Some(2)), CMFlow("IM1", Some(4)))

    val cm1 = if (shortTests) List(cm1List(rnd.nextInt(cm1List.length))) else cm1List
    val cm2 = if (shortTests) List(cm2List(rnd.nextInt(cm2List.length))) else cm2List
    val cm3 = if (shortTests) List(cm3List(rnd.nextInt(cm3List.length))) else cm3List
    val cm4 = if (shortTests) List(cm4List(rnd.nextInt(cm4List.length))) else cm4List

    println("cm1: " + cm1 + " cm2: " + cm2 + " cm3: " + cm3 + " cm4: " + cm4)

    val prel = if (shortTests) List(rcu_misint_preconv_prel(rnd.nextInt(rcu_misint_preconv_prel.length))) else rcu_misint_preconv_prel
    val conv = if (shortTests) List(rcu_misint_preconv_conv(rnd.nextInt(rcu_misint_preconv_conv.length))) else rcu_misint_preconv_conv
    val pk = if (shortTests) List(rcu_misint_preconv_pk(rnd.nextInt(rcu_misint_preconv_pk.length))) else rcu_misint_preconv_pk

    println("prel: " + prel + " conv: " + conv + " pk: " + pk)

    for (
      cmflow1 <- cm1;
      cmflow2 <- cm2;
      cmflow3 <- cm3;
      cmflow4 <- cm4
    ) {
      val measures = measuresList :::
        getMockFlow(cmflow1, dates.head, pdr1) :::
        getMockFlow(cmflow2, dates(1), pdr1) :::
        getMockFlow(cmflow3, dates(2), pdr1) :::
        getMockFlow(cmflow4, dates(3), pdr1) :::
        getMockFlow(cmflow1, dates.head, pdr2, None) :::
        getMockFlow(cmflow2, dates(1), pdr2, None) :::
        getMockFlow(cmflow3, dates(2), pdr2, None) :::
        getMockFlow(cmflow4, dates(3), pdr2, None)


      val measuresRDD = Environment.getSparkContext.parallelize(measures.map(_.asInstanceOf[Flow]))

      val res = controller.putForcingCodeToMeasures(measuresRDD).collect()

      res.filter(_.pdr == pdr1).sortBy(_.date).foreach(f => println(f.forcing + " - " + f))
      println()
      res.filter(_.pdr == pdr2).sortBy(_.date).foreach(f => println(f.forcing + " - " + f))

      for (misint_preconv <- prel) {
        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)


        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)
        consumptions2.foreach(println)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          val tipoForzatura = {
            if (date < dates.head) Some(cmflow1.service + "PRE" + cmflow1.cau_int_cor.get)
            else if (date < dates(1)) Some(cmflow2.service + "PRE" + cmflow2.cau_int_cor.get)
            else if (date < dates(2)) Some(cmflow2.service + "POST" + cmflow2.cau_int_cor.get)
            else None
          }
          Assert.assertEquals(tipoForzatura, f.tipoForzatura)
          if (!dates.contains(endDate) && !dates.contains(date))
            Assert.assertEquals(f.tipoForzatura, tipoForzatura)
          if (date < dates(1)) checkConsumption(f, 100, 100)
          else if (date < dates(2)) checkConsumption(f, 2, 2)
          else if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 1, 1)
        })
        consumptions2.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (dates.contains(date) || dates.contains(endDate)) {}
          else if (date > dates(2))
            checkConsumption(f, 1, 1)
          else checkConsumption(f, 2, 2)
        })
      }

      for (misint_preconv <- conv) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)
        consumptions2.foreach(println)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (date < dates(1)) checkConsumption(f, 100, 100)
          else if (date < dates(2)) checkConsumption(f, 2, 2)
          else if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 100, 100)
        })
        // IN QUESTO CASO NON CONTROLLO CHE I CONSUMI SUCCESSIVI ABBIANO K
        // PERCHé LE REGOLE MI DICONO DI PRENDERE LA CONVERTITA E NON CONTROLLANO (VECCHIO CODICE)
        // SE LA CONVERTITA ESISTE NELLA MISURA
        consumptions2.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (!dates.contains(date) && !dates.contains(endDate) && date < dates(2))
            checkConsumption(f, 2, 2)
        })
      }

      for (misint_preconv <- pk) {

        val rcuTechval = rcuTech(misint_preconv._1, misint_preconv._2)
        val result = new ConsumptionController().execute(measuresRDD, rcuTechval, rcusCa, rcuGasProfilo)

        val consumptions = result.flatMap(_._2._1)
          .filter(_.pdr == pdr1)
          .collect.sortBy(_.startSegment.getTime)

        val consumptions2 = result.flatMap(_._2._1)
          .filter(_.pdr == pdr2)
          .collect.sortBy(_.startSegment.getTime)

        consumptions.foreach(println)
        consumptions2.foreach(println)

        consumptions.foreach(f => {
          val date = format.format(f.startSegment)
          val endDate = format.format(f.endSegment)
          if (date < dates(1)) checkConsumption(f, 100, 100)
          else if (date < dates(2)) checkConsumption(f, 2, 2)
          else if (!dates.contains(date) && !dates.contains(endDate))
            checkConsumption(f, 2, 2)
        })
        consumptions2.foreach(f => {
          checkConsumption(f, 2, 2)
        })

      }

    }
  }

}

object ConsumptionControllerTestIntegrazioneForzature {
  val rnd: Random = new Random()

  case class CMFlow2(pdr: String, date: String, cau_int_cor: Option[Int])

  case class CMFlow(service: String, cau_int_cor: Option[Int])

  def getMockFlow(cmflow1: CMFlow, date: String, pdr: String, converted: Option[Double] = Some(100)): List[Flow] = {
    if (cmflow1.service == "IM1")
      MockFlow.generateIm1(pdr, format.parse(date), cau_int_cor = cmflow1.cau_int_cor, coeffPre = 2.0, coeffPost = 2.0,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), converted = converted)
    else
      MockFlow.generateIgmg(pdr, format.parse(date), cau_int_cor = cmflow1.cau_int_cor, coeffPre = 2.0, coeffPost = 2.0,
        serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), converted = converted)
  }

  def generateIm1(pdr1: String, date: String, cor: Option[Int], converted: Option[Double] = Some(100)) = {
    MockFlow.generateIm1(pdr1, format.parse(date), cau_int_cor = cor, coeffPre = 2.0, coeffPost = 2.0,
      serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), converted = converted)
  }

  def generateIgmg(pdr1: String, date: String, cor: Option[Int], converted: Option[Double] = Some(100)) = {
    MockFlow.generateIgmg(pdr1, format.parse(date), cau_int_cor = cor, coeffPre = 2.0, coeffPost = 2.0,
      serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), converted = converted)
  }

  def format = Constants.getFormatter("yyyy-MM-dd")

  val pdr1: String = "1"
  val pdr2: String = "2"


  val rcuGasProfilo = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(n_id_pdr = "11", d_data_inizio = Some(format.parse("2007-01-01")), d_data_fine = Some(format.parse("2021-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None"),
    RcuGasProfilo(n_id_pdr = "22", d_data_inizio = Some(format.parse("2007-01-01")), d_data_fine = Some(format.parse("2021-01-01")), n_id_var_profilo = "12", t_anno = 2000, t_cod_profilo = "None", t_cod_cat_uso = "None", t_cod_classe_prelievo = "None")
  ))

  val rettificheList = List(
    Rml(pdr = "1", service = "RML", motivation = Some(4), date = Some(format.parse("2008-01-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Rgl(pdr = "1", service = "RGL", motivation = Some(5), date = Some(format.parse("2008-02-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Rgl(pdr = "1", service = "RGL", motivation = Some(4), date = Some(format.parse("2008-03-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Rml(pdr = "1", service = "RML", motivation = Some(4), date = Some(format.parse("2008-04-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Rgl(pdr = "1", service = "RGL", motivation = Some(4), date = Some(format.parse("2008-05-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Rgl(pdr = "1", service = "RGL", motivation = Some(4), date = Some(format.parse("2008-06-10")), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, collected = None, pivaDistr = None, pivaUtente = None, ammissibilita = None)
  )

  val measuresList = List(
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-01-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-02-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2008-03-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-04-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-05-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2008-06-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2008-07-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2008-08-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-09-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-10-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2008-11-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2008-12-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "1", service = "TGL", date = Some(format.parse("2009-01-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2009-02-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2009-03-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "1", service = "TML", date = Some(format.parse("2009-04-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-01-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-02-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2008-03-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-04-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-05-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2008-06-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2008-07-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2008-08-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-09-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-10-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2008-11-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = Some(100), serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2008-12-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tgl(pdr = "2", service = "TGL", date = Some(format.parse("2009-01-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2009-02-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2009-03-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
    Tml(pdr = "2", service = "TML", date = Some(format.parse("2009-04-10")), readType = Some('E'), isValid = Some("SI"), measure = Some(1), converted = None, serialNumberConv = Some("MIT0030008923266"), serialNumberMis = Some("21901547"), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_03707740233/2019/1108/00489490011_03707740233_201910_TGL0050_20191108134500_1.xml"), d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
  )

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata
  val rcu_misint_preconv_prel: List[(Option[String], Option[String])] = List((Some("SI"), None), (Some("SI"), Some("NO")),
    (Some("SI"), Some("SI")), (Some("S"), Some("NO")), (Some(""), Some("NO")))

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Convertita
  val rcu_misint_preconv_conv: List[(Option[String], Option[String])] = List((Some("NO"), Some("SI")),
    (None, Some("SI")), (Some("N"), Some("SI")), (Some(""), Some("SI")), (None, None))

  // combinazioni (GRUPPO_MIS_INT,PRE_CONV) per cui per il segmento Post considero segnante Prelevata*coeff
  val rcu_misint_preconv_pk: List[(Option[String], Option[String])] = List(
    (Some("NO"), Some("NO")), (Some("N"), Some("NO")))


  val rcusCa = Environment.getSparkContext.parallelize(List(
    RcuGasMassivo(startDate = format.parse("2007-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "11"),
    RcuGasMassivo(startDate = format.parse("2007-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = null,
      t_processo = null, id_regione_climatica = 0, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null, n_id_pdr = "22")
  ))

  val rcusVarProf = Environment.getSparkContext.parallelize(List(
    RcuGasProfilo(d_data_inizio = Some(format.parse("2007-01-01")), d_data_fine = Some(format.parse("2021-01-01")), n_id_var_profilo = "", n_id_pdr = "11", t_cod_cat_uso = null, t_cod_profilo = null, t_anno = 0, t_cod_classe_prelievo = null),
    RcuGasProfilo(d_data_inizio = Some(format.parse("2007-01-01")), d_data_fine = Some(format.parse("2021-01-01")), n_id_var_profilo = "", n_id_pdr = "22", t_cod_cat_uso = null, t_cod_profilo = null, t_anno = 0, t_cod_classe_prelievo = null)
  ))

  def rcuTech(gruppo_mis_int: Option[String] = Some(""), pre_conv: Option[String] = Some("")): RDD[RcuGasMassivoTech] = Environment.getSparkContext.parallelize(List(
    RcuGasMassivoTech(startDate = format.parse("2007-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "1", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None),
    RcuGasMassivoTech(startDate = format.parse("2007-01-01"), endDate = format.parse("2020-01-05"), t_codice_pdr = "2", n_coeff_correzione = Some(2),
      t_misuratore_integrato = gruppo_mis_int, t_pre_conv = pre_conv, n_num_cifre_convertitore = None, n_num_cifre_misuratore = None)
  ))

  def checkConsumption(c: Consumption, startValue: Double, endValue: Double): Unit = {
    Assert.assertEquals(startValue, c.startvalue, 0)
    Assert.assertEquals(endValue, c.endvalue, 0)
  }*/

}
