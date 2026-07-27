package it.au.misure.ee_switching.flow

import it.au.misure.ee_switching.utility.{EnvironmentSparkTest}


class TestExampleFlow extends EnvironmentSparkTest {
  def testRun(): Unit = {
    ExampleFlow().run()
  }
}
