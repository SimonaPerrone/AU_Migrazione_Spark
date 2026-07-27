package it.eng.au.aggregatoreConsumiCommon.util

import org.apache.spark.sql.Row
import org.junit.Assert

trait Check {

  def checkResultRowUDD(r: Row,
                        pivaDistr: String,
                        pivaUdb: String,
                        dtg: String,
                        codRemi: String,
                        idRegClim: String,
                        codProfStd: String,
                        treatment: String,
                        tipoCliente: String,
                        unitMisPrel: String,
                        pivaUdd: String,
                        annoMese: String,
                        pair: Tuple2[Int, Int]
                       ): Unit = {
    Assert.assertTrue(r.getString(0) == pivaDistr)
    Assert.assertTrue(r.getString(1) == pivaUdb)
    Assert.assertTrue(r.getString(2) == dtg)
    Assert.assertTrue(r.getString(3) == codRemi)
    Assert.assertTrue(r.getString(4) == idRegClim)
    Assert.assertTrue(r.getString(5) == codProfStd)
    Assert.assertTrue(r.getString(6) == treatment)
    Assert.assertTrue(r.getString(7) == tipoCliente)
    Assert.assertTrue(r.getString(8) == unitMisPrel)
    Assert.assertTrue(r.getString(9) == pivaUdd)
    Assert.assertTrue(r.getString(10) == annoMese)

    val d = r.get(10 + pair._1)
    Assert.assertTrue(d.asInstanceOf[Long] == pair._2)

  }

  def checkResultRowID(r: Row,
                       annoMese: String,
                       dataVal: String,
                       pivaDistr: String,
                       pivaUdd: String,
                       pivaUdb: String,
                       dtg: String,
                       codRemi: String,
                       idRegClim: String,
                       codProfStd: String,
                       treatment: String,
                       tipoCliente: String,
                       unitMisPrel: String,
                       pair: (Int, Int)
                      ): Unit = {
    Assert.assertTrue(r.getString(0) == annoMese)
    Assert.assertTrue(r.getString(1) == dataVal)
    Assert.assertTrue(r.getString(2) == pivaDistr)
    Assert.assertTrue(r.getString(3) == pivaUdd)
    Assert.assertTrue(r.getString(4) == pivaUdb)
    Assert.assertTrue(r.getString(5) == dtg)
    Assert.assertTrue(r.getString(6) == codRemi)
    Assert.assertTrue(r.getString(7) == idRegClim)
    Assert.assertTrue(r.getString(8) == codProfStd)
    Assert.assertTrue(r.getString(9) == treatment)
    Assert.assertTrue(r.getString(10) == tipoCliente)
    Assert.assertTrue(r.getString(11) == unitMisPrel)

    val day = r.getInt(11 + pair._1)
    Assert.assertTrue(day == pair._2)
  }

  def checkResultRowRDB(r: Row,
                        annoMese: String,
                        dataVal: String,
                        pivaRdb: String,
                        pivaIt: String,
                        pivaUdd: String,
                        pivaUdb: String,
                        dtg: String,
                        codRemi: String,
                        idRegClim: String,
                        codProfStd: String,
                        treatment: String,
                        tipoCliente: String,
                        unitMisPrel: String,
                        pair: (Int, Int)
                       ): Unit = {
    Assert.assertTrue(r.getString(0) == annoMese)
    Assert.assertTrue(r.getString(1) == dataVal)
    Assert.assertTrue(r.getString(2) == pivaRdb)
    Assert.assertTrue(r.getString(3) == pivaIt)
    Assert.assertTrue(r.getString(4) == pivaUdd)
    Assert.assertTrue(r.getString(5) == pivaUdb)
    Assert.assertTrue(r.getString(6) == dtg)
    Assert.assertTrue(r.getString(7) == codRemi)
    Assert.assertTrue(r.getString(8) == idRegClim)
    Assert.assertTrue(r.getString(9) == codProfStd)
    Assert.assertTrue(r.getString(10) == treatment)
    Assert.assertTrue(r.getString(11) == tipoCliente)
    Assert.assertTrue(r.getString(12) == unitMisPrel)

    val day = r.getInt(12 + pair._1)
    Assert.assertTrue(day == pair._2)
  }

  def checkResultRowIT(r: Row,
                       annoMese: String,
                       dataVal: String,
                       pivaIt: String,
                       pivaUdd: String,
                       pivaUdb: String,
                       dtg: String,
                       codRemi: String,
                       idRegClim: String,
                       codProfStd: String,
                       treatment: String,
                       tipoCliente: String,
                       unitMisPrel: String,
                       pair: (Int, Int)
                      ): Unit = {
    Assert.assertTrue(r.getString(0) == annoMese)
    Assert.assertTrue(r.getString(1) == dataVal)
    Assert.assertTrue(r.getString(2) == pivaIt)
    Assert.assertTrue(r.getString(3) == pivaUdd)
    Assert.assertTrue(r.getString(4) == pivaUdb)
    Assert.assertTrue(r.getString(5) == dtg)
    Assert.assertTrue(r.getString(6) == codRemi)
    Assert.assertTrue(r.getString(7) == idRegClim)
    Assert.assertTrue(r.getString(8) == codProfStd)
    Assert.assertTrue(r.getString(9) == treatment)
    Assert.assertTrue(r.getString(10) == tipoCliente)
    Assert.assertTrue(r.getString(11) == unitMisPrel)

    val day = r.getInt(11 + pair._1)
    Assert.assertTrue(day == pair._2)
  }

  def checkResultRowUDB(r: Row,
                        annoMese: String,
                        dataVal: String,
                        pivaDistr: String,
                        pivaUdd: String,
                        pivaUdb: String,
                        dtg: String,
                        codRemi: String,
                        idRegClim: String,
                        codProfStd: String,
                        treatment: String,
                        tipoCliente: String,
                        unitMisPrel: String,
                        pair: (Int, Int)
                       ): Unit = {
    Assert.assertTrue(r.getString(0) == annoMese)
    Assert.assertTrue(r.getString(1) == dataVal)
    Assert.assertTrue(r.getString(2) == pivaDistr)
    Assert.assertTrue(r.getString(3) == pivaUdd)
    Assert.assertTrue(r.getString(4) == pivaUdb)
    Assert.assertTrue(r.getString(5) == dtg)
    Assert.assertTrue(r.getString(6) == codRemi)
    Assert.assertTrue(r.getString(7) == idRegClim)
    Assert.assertTrue(r.getString(8) == codProfStd)
    Assert.assertTrue(r.getString(9) == treatment)
    Assert.assertTrue(r.getString(10) == tipoCliente)
    Assert.assertTrue(r.getString(11) == unitMisPrel)

    val day = r.getInt(11 + pair._1)
    Assert.assertTrue(day == pair._2)
  }
}
