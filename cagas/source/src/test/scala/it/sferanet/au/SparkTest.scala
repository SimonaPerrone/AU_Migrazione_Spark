package it.sferanet.au

import it.sferanet.au.utilities.Environment
import junit.framework.TestCase

import java.io.{File, FileInputStream}
import java.lang.reflect.Field
import java.util.Properties

trait SparkTest extends TestCase {
/*
}

object SparkTest {
  //val env = TestEnv("src/test/resources/config.properties")
}

case class TestEnv(settingsPath: String) extends Environment(settingsPath = settingsPath) {

  def setProperty(key: String, value: String): Unit = {
    val settingsField: Field = this.getClass.getSuperclass.getDeclaredField("_settings")
    settingsField.setAccessible(true)
    settingsField.get(this).asInstanceOf[Properties].setProperty(key, value)
  }

  def resetProperty(): Unit = {
    val p = new Properties()
    p.load(new FileInputStream(new File(settingsPath)))
    val enum = p.propertyNames()
    while (enum.hasMoreElements) {
      val key = enum.nextElement()
      val value = p.getProperty(key.toString)
      Environment.setProperty(key.toString, value)
    }
  }
*/
}
