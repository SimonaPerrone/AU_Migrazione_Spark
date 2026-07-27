package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarPrelAnnuoP
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarPrelAnnuoPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType}
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Contiene il valore del prelievo annuo per un dato PdR */
class RcuGasVarPrelAnnuoPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasVarPrelAnnuoPath
  override val columns: List[String] = List(
    RcuGasVarPrelAnnuoPSchema.n_id_pdr,
    RcuGasVarPrelAnnuoPSchema.d_data_inizio,
    RcuGasVarPrelAnnuoPSchema.d_data_fine,
    RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo
  )

  def get(startDate: String, endDate: String): RDD[RcuGasVarPrelAnnuoP] = {

    filterDfWithStartEndDate(readParquet
      , RcuGasVarPrelAnnuoPSchema.d_data_inizio.toString
      , RcuGasVarPrelAnnuoPSchema.d_data_fine.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startDate
      , endDate
      , "yyyyMM"
    )
      .withColumn(RcuGasVarPrelAnnuoPSchema.d_data_inizio, col(RcuGasVarPrelAnnuoPSchema.d_data_inizio).cast(StringType))
      .withColumn(RcuGasVarPrelAnnuoPSchema.d_data_fine, col(RcuGasVarPrelAnnuoPSchema.d_data_fine).cast(StringType))
      .select(col(RcuGasVarPrelAnnuoPSchema.n_id_pdr), col(RcuGasVarPrelAnnuoPSchema.d_data_inizio), col(RcuGasVarPrelAnnuoPSchema.d_data_fine), col(RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo)
      )
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasVarPrelAnnuoP = r => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarPrelAnnuoPSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
    }
    val endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarPrelAnnuoPSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31").toDateTimeAtStartOfDay
    }

    RcuGasVarPrelAnnuoP(
      nIdPdr = r.getAs[String](RcuGasVarPrelAnnuoPSchema.n_id_pdr),
      dataInizio = startDate,
      dataFine = endDate,
      nPrelivevoAnnuo = Try(Option(r.getAs[String](RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo)).map(_.toDouble)).getOrElse(None)
    )
  }
}
