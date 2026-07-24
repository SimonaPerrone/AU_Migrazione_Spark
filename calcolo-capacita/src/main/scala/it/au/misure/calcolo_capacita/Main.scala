package it.au.misure.calcolo_capacita

import it.au.misure.calcolo_capacita.component.hdao.CalcoloCtcFlowInputHDao
import it.au.misure.calcolo_capacita.component.utility.SparkImplicit
import it.au.misure.calcolo_capacita.flow.CalcoloCtcFlow
import org.joda.time.LocalDateTime


object Main extends SparkImplicit {

  def main(args: Array[String]): Unit = {

    val executionId: String = LocalDateTime.now().toString("yyyyMMddHHmmss")

    val ctcFlow = CalcoloCtcFlow(args, CalcoloCtcFlowInputHDao(), executionId)
    val result = ctcFlow.run()
    ctcFlow.write(result)


  }


}
