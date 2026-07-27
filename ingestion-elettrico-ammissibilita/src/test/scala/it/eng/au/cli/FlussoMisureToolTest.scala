package it.eng.au.cli

import it.eng.au.utility.SystemUtility

object FlussoMisureToolTest {
  SystemUtility.setLocalLaunch()

  def main(args: Array[String]): Unit = {
    val arguments: Array[String] = Array(
      "-ia",
      "-g",
      "-l",
      "-V",
      "-y", "2020",
      "-m", "09",
      "-s", "23"
    )

    //FlussoMisureTool.main(arguments)
  }
}
