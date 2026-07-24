package it.eng.au.cceCalcolo.args

import it.eng.au.cceCalcolo.utility.log.LogUtility
import scopt.OptionParser

object ArgsFactory {

  def parse(args: Array[String]): CalcoloArgs = {
    val parser = new OptionParser[CalcoloArgs]("[CCE] Calcolo") {
      head("cce-calcolo")

      opt[Int]('a', "anno-calc")
        .action((value, c) => c.copy(annoCalc = Some(value)))
        .text("Value for annoCalc")
        .validate(annoCalc =>
          if (annoCalc.isInstanceOf[Int])
            success
          else
            failure("variable annoCalc must be integer type"))

      opt[Int]('m', "mese-calc")
        .action((value, c) => c.copy(meseCalc = Some(value)))
        .text("Value for meseCalc")
        .validate(meseCalc =>
          if (meseCalc.isInstanceOf[Int])
            success
          else
            failure("variable meseCalc must be integer type"))

      opt[String]('t', "tipo-calc")
        .action((value, c) => c.copy(tipoCalc = Some(value)))
        .text("Value for tipoCalc")
        .validate(tipoCalc =>
          if (tipoCalc.equals("P") || tipoCalc.equals("PR") || tipoCalc.equals("Pein") || tipoCalc.equals("PRein"))
            success
          else
            failure("variable tipoCalc must be one of the following values: P,PR,Pein,PRein"))

      opt[Boolean]('f', "massivo-flag")
        .action((value, c) => c.copy(massivoFlag = Some(value)))
        .text("Value for massivoFlag")
        .validate(massivoFlag =>
          if (massivoFlag.isInstanceOf[Boolean])
            success
          else
            failure("variable massivoFlag must be one of the following values: true or false"))

      help('h', "help").text("prints all options")

      checkConfig(c =>
        if (c.tipoCalc.isEmpty || ((c.tipoCalc.getOrElse("").equals("PR") || c.tipoCalc.getOrElse("").equals("PRein")) && c.massivoFlag.isEmpty)) failure(s"""One or more missing parameters""")
        else success)
    }

    parser.parse(args, CalcoloArgs()) match {
      case Some(c) => c
      case None =>
        LogUtility.printError(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}
