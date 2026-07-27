package it.au.misure.ee_switching.flow

import it.au.misure.ee_switching.utility.environment.Environment

case class ExampleFlow() {
  def run(): Unit = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext
    println("fatto")
  }
}
