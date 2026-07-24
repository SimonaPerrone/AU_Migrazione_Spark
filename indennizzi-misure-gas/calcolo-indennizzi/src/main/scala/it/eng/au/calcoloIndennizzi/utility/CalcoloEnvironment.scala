package it.eng.au.calcoloIndennizzi.utility

import it.eng.au.calcoloIndennizzi.utility.args.Args
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{CALCOLO_APPLICATION_NAME, CIG_LOG}
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.getDaysFromMonth
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.log4j.Logger
import org.joda.time.DateTime

object CalcoloEnvironment {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /** Inizializza Spark e setta alcune properties base richiamate poi durante il processo. */
  def setEnvironment(parsedArgs: Args): Unit = {
    val applicationName = CALCOLO_APPLICATION_NAME
    val logName = CIG_LOG

    Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath, needsKryo = true)

    val sysDate = new DateTime(Environment.executionId)

    /** Indica l'anno-mese di cui effettuare il calcolo. In particolare,
     *  - se è definito da parametro, allora lo estrae da lì;
     *  - altrimenti, utilizza l'intero [[getMonthDifferenceTimeBack]] per calcolarlo automaticamente dal mese corrente. */
    val yearMonth = parsedArgs.yearMonth.getOrElse(sysDate.minus(Properties.getMonthDifferenceTimeBack).toString("yyyyMM"))
    /** Indica il giorno di soglia di lettura delle TGL. Può essere passato come input al processo o letto dalle properties. */
    val thresholdDay = parsedArgs.thresholdDay.map(_.toInt).getOrElse(Environment.getProperty("tgl.dayOfMonth.threshold"))
    logger.warn(s"$CIG_LOG Year-month to compute: $yearMonth")
    logger.warn(s"$CIG_LOG TGL Threshold day: $thresholdDay")

    Environment.setProperty("year.month", yearMonth)
    Environment.setProperty("tgl.dayOfMonth.threshold", thresholdDay.toString)
    Environment.setProperty("days.in.month", getDaysFromMonth(Properties.getYearMonth))
    Environment.setProperty("recovery.mode", parsedArgs.recoveryMode.toString)
  }
}
