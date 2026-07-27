package it.au.misure.ee_switching.flow

import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.flow.FunzionaliFlow.emptyCheck
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.EnvironmentSparkTest
import org.junit.Test

class TestFlow extends EnvironmentSparkTest {

  def assertThrows[E](f: => Unit)(implicit eType:scala.reflect.ClassTag[E]): Unit = {
    try {
      f
    } catch {
      case e: Exception =>
        if ( eType.runtimeClass.isAssignableFrom(e.getClass))
          return;
    }
    throw new AssertionError("Expected error of type " + eType.runtimeClass.getName )
  }

  @Test
  def testEmptyCheck(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    assertThrows[IllegalArgumentException] { emptyCheck(Environment.getSpark.sparkContext.emptyRDD[String].toDF(), FlowArgsConfig()) }
    assertThrows[IllegalArgumentException] { emptyCheck(Environment.getSpark.sparkContext.parallelize(Seq.empty[String]).toDF(), FlowArgsConfig()) }
  }

}
