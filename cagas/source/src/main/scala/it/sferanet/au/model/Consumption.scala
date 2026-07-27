package it.sferanet.au.model

import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

case class Consumption(
                        pdr: String,
                        startService: String,
                        endService: String,
                        startSegment: Timestamp,
                        endSegment: Timestamp,
                        var startvalue: Double,
                        var endvalue: Double,
                        startLocalFile: Option[String],
                        endLocalFile: Option[String],
                        idConsumptionErrorState: ConsumptionErrorStates.Value,
                        nCoeffCorrezione: Option[Double],
                        startTMisuratoreIntegrato: Option[String],
                        endTTMisuratoreIntegrato: Option[String],
                        startTPreConv: Option[String],
                        endTPreConv: Option[String],
                        startTCodProf: Option[String],
                        nPrelievoAnnuo: String,
                        tipoCoeff: String = null,
                        coerenzaDim: String = null,
                        tipoForzatura: Option[String] = None,
                        startDateZ: Timestamp = null,
                        endDateZ: Timestamp = null
                      ) {

  def setStartValue(startvalue: Double): Unit = {
    this.startvalue = startvalue
  }

  def setEndValue(endvalue: Double): Unit = {
    this.endvalue = endvalue
  }

  def getConsumption(rcu: RcuGasMassivoTech): Option[Double] = {
    val retVal = endvalue - startvalue
    //    if (retVal < 0) {
    //      if (rcu.n_num_cifre_misuratore.isDefined) {
    //        val maxValue = "".padTo(rcu.n_num_cifre_misuratore.get, "9").mkString("").toInt // TODO gestire il giro contatore
    //        if (maxValue + endvalue > startvalue)
    //          retVal = endvalue + (maxValue - startvalue)
    //      }
    //    }
    if (retVal < 0)
      None
    else
      Some(retVal)
  }

  def getNumberOfDays: Int = {
    TimeUnit.DAYS.convert(endSegment.getTime - startSegment.getTime, TimeUnit.MILLISECONDS).toInt
  }

}

object Consumption {
  val schema: StructType =
    StructType(
      StructField("pdr", StringType, nullable = false) ::
        StructField("startService", StringType, nullable = true) ::
        StructField("endService", StringType, nullable = true) ::
        StructField("startSegment", TimestampType, nullable = true) ::
        StructField("endSegment", TimestampType, nullable = true) ::
        StructField("startvalue", DoubleType, nullable = true) ::
        StructField("endvalue", DoubleType, nullable = true) ::
        StructField("idConsumptionErrorState", IntegerType, nullable = false) ::
        StructField("n_coeff_correzione", DoubleType, nullable = true) ::
        StructField("t_misuratore_integrato", StringType, nullable = true) ::
        StructField("end_t_misuratore_integrato", StringType, nullable = true) ::
        StructField("t_pre_conv", StringType, nullable = true) ::
        StructField("end_t_pre_conv", StringType, nullable = true) ::
        StructField("t_cod_prof", StringType, nullable = true) ::
        StructField("n_prelievo_annuo", StringType, nullable = true) ::
        StructField("tipo_coeff", StringType, nullable = true) ::
        StructField("tipo_forzatura", StringType, nullable = true) ::
        StructField("coerenza_dim", StringType, nullable = true) :: Nil)

  def empty(pdr: String): Consumption = new Consumption(pdr, "", "", new Timestamp(0), new Timestamp(Long.MaxValue), 0.0, 0.0, None, None, ConsumptionErrorStates.None, None, None, None, None, None, None, "")
}
