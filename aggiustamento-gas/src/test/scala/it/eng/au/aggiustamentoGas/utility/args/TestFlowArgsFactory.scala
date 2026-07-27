package it.eng.au.aggiustamentoGas.utility.args

import junit.framework.TestCase
import org.joda.time.DateTime
import org.junit.Assert

class TestFlowArgsFactory extends TestCase{

  def test(): Unit = {
    val res = FlowArgsFactory.parse(Array("-p", "path/to/param", "-s", "CCG", "-d", "2022-05-19"))

    Assert.assertEquals("path/to/param", res.pathToProperties)
    Assert.assertEquals("CCG", res.session)
    Assert.assertEquals(new DateTime("2022-05-19"), res.dateToRun.get)
  }

  def test2(): Unit = {
    val res = FlowArgsFactory.parse(Array("-p", "path/to/param", "-d", "2022-05-19"))

    Assert.assertEquals("path/to/param", res.pathToProperties)
    Assert.assertEquals("AGG", res.session)
    Assert.assertEquals(new DateTime("2022-05-19"), res.dateToRun.get)
  }
}
