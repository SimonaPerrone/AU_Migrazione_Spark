package it.au.misure.calcolo_capacita.component.utility.`implicit`

import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{errorDataCalcNotCompForm, errorJodaTimeFormat}
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.{Failure, Success, Try}

object ConvertStringIntoDate {

  implicit class ConvertString(stringDate: String) {
    def getLocalDate(format: String): LocalDate = {
      convertDate(stringDate, format)
    }

    private def convertDate(dataCalc: String, format: String): LocalDate = {
      val dataFormatted = formatData(format)
      dataFormatted match {
        case Success(formatter) => {
          val getData = Try(LocalDate.parse(dataCalc, formatter))
          getData match {
            case Success(data) => data
            case Failure(data) => {
              LoggerUtility.printError(f"The format $dataCalc is not compatible with $format, data wrong: $data", getClass.getName)
              System.exit(errorDataCalcNotCompForm)
              ???
            }
          }
        }
        case Failure(nd) => LoggerUtility.printError(f"The format $format is not compatible with Joda Time, wrong format: $nd", getClass.getName)
          System.exit(errorJodaTimeFormat)
          ???
      }

    }

    private def formatData(format: String): Try[DateTimeFormatter] = {
      Try(DateTimeFormat.forPattern(format))
    }

  }

}