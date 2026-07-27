package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarConvertitore
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarConvertitorePSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

class RcuGasVarConvertitorePDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasVarConvertitorePath
  override val columns: List[String] = List(
    RcuGasVarConvertitorePSchema.n_id_pdr,
    RcuGasVarConvertitorePSchema.n_num_cifre_convertitore,
    RcuGasVarConvertitorePSchema.d_data_inizio,
    RcuGasVarConvertitorePSchema.d_data_fine
  )

  def get(startDate: String, endDate: String): RDD[RcuGasVarConvertitore] = {
    val df = this.readParquet
      .selectExpr(columns:_*)

    filterDfWithStartEndDate(df
      , RcuGasVarConvertitorePSchema.d_data_inizio.toString
      , RcuGasVarConvertitorePSchema.d_data_fine.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startDate
      , endDate
      , "yyyyMM"
    )
      .withColumn(RcuGasVarConvertitorePSchema.d_data_inizio, col(RcuGasVarConvertitorePSchema.d_data_inizio).cast(StringType))
      .withColumn(RcuGasVarConvertitorePSchema.d_data_fine, col(RcuGasVarConvertitorePSchema.d_data_fine).cast(StringType))
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasVarConvertitore = r => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarConvertitorePSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
    }
    val endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarConvertitorePSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31").toDateTimeAtStartOfDay
    }

    RcuGasVarConvertitore(
      nIdPdr = r.getAs[String](RcuGasVarConvertitorePSchema.n_id_pdr),
      tPreConv = Some("SI"),
      startDateConv = startDate,
      endDateConv = endDate,
      nCifreConv = Try(Option(r.getAs[String](RcuGasVarConvertitorePSchema.n_num_cifre_convertitore)).map(_.toInt)).getOrElse(None)
    )
  }

}
