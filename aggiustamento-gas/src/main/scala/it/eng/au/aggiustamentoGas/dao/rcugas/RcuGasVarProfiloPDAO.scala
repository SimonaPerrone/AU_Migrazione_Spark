package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarProfiloP
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarProfiloPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Contiene il codice profilo per un dato PdR */
class RcuGasVarProfiloPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasVarProfiloPath
  override val columns: List[String] = List(
    RcuGasVarProfiloPSchema.n_id_pdr,
    RcuGasVarProfiloPSchema.d_data_inizio,
    RcuGasVarProfiloPSchema.d_data_fine,
    RcuGasVarProfiloPSchema.t_cod_profilo
  )

  def get(startDate: String = "01/01/1970", endDate: String = "31/12/2999"): RDD[RcuGasVarProfiloP] = {
    filterDfWithStartEndDate(readParquet
      , RcuGasVarProfiloPSchema.d_data_inizio.toString
      , RcuGasVarProfiloPSchema.d_data_fine.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startDate
      , endDate
      , "yyyyMM"
    )
      .withColumn(RcuGasVarProfiloPSchema.d_data_inizio, col(RcuGasVarProfiloPSchema.d_data_inizio).cast(StringType))
      .withColumn(RcuGasVarProfiloPSchema.d_data_fine, col(RcuGasVarProfiloPSchema.d_data_fine).cast(StringType))
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasVarProfiloP = r => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarProfiloPSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
    }
    val endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarProfiloPSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31").toDateTimeAtStartOfDay
    }

    RcuGasVarProfiloP(
      nIdPdr = r.getAs[String](RcuGasVarProfiloPSchema.n_id_pdr),
      dataInizio = startDate,
      dataFine = endDate,
      tCodProfilo = Option(r.getAs[String](RcuGasVarProfiloPSchema.t_cod_profilo))
    )
  }
}
