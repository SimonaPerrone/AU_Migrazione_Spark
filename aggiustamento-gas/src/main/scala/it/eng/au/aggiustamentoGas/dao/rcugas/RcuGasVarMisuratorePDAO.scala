package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasTech
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasTechSchema, RcuGasVarConvertitorePSchema, RcuGasVarMisuratorePSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{coalesce, col, from_unixtime, lit, unix_timestamp, when}
import org.apache.spark.sql.types.{DateType, StringType}
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Contiene le informazioni sulla classe misuratore per un dato PdR */
class RcuGasVarMisuratorePDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasVarMisuratorePath
  override val columns: List[String] = List(
    RcuGasVarMisuratorePSchema.n_id_pdr,
    RcuGasVarMisuratorePSchema.t_misuratore_integrato,
    RcuGasVarMisuratorePSchema.t_classe_misuratore,
    RcuGasVarMisuratorePSchema.n_coeff_correzione,
    RcuGasVarMisuratorePSchema.n_num_cifre_misuratore,
    RcuGasVarMisuratorePSchema.d_data_inizio,
    RcuGasVarMisuratorePSchema.d_data_fine
  )

  def get(startDate: String, endDate: String): RDD[RcuGasTech] = {
    val df = this.readParquet

    filterDfWithStartEndDate(df
      , RcuGasVarMisuratorePSchema.d_data_inizio.toString
      , RcuGasVarMisuratorePSchema.d_data_fine.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startDate
      , endDate
      , "yyyyMM"
    )
      .withColumn(RcuGasTechSchema.data_inizio_tech, col(RcuGasVarMisuratorePSchema.d_data_inizio))
      .withColumn(RcuGasTechSchema.data_fine_tech, col(RcuGasVarMisuratorePSchema.d_data_fine))
      .withColumn(RcuGasTechSchema.data_inizio_tech, col(RcuGasTechSchema.data_inizio_tech).cast(StringType))
      .withColumn(RcuGasTechSchema.data_fine_tech, col(RcuGasTechSchema.data_fine_tech).cast(StringType))
      .select(col(RcuGasTechSchema.n_id_pdr)
        , col(RcuGasTechSchema.t_misuratore_integrato)
        , col(RcuGasTechSchema.t_classe_misuratore)
        , col(RcuGasTechSchema.n_coeff_correzione)
        , col(RcuGasTechSchema.data_inizio_tech)
        , col(RcuGasTechSchema.data_fine_tech)
        , col(RcuGasTechSchema.n_num_cifre_misuratore)
      )
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasTech = r => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasTechSchema.data_inizio_tech)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
    }
    val endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasTechSchema.data_fine_tech)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31").toDateTimeAtStartOfDay
    }

    RcuGasTech(
      nIdPdr = r.getAs[String](RcuGasTechSchema.n_id_pdr),
      gruppoMisInt = Option(r.getAs[String](RcuGasTechSchema.t_misuratore_integrato)),
      classeMisuratore = Option(r.getAs[String](RcuGasTechSchema.t_classe_misuratore)),
      nCoeffCorr = Try(Option(r.getAs[String](RcuGasTechSchema.n_coeff_correzione)).map(_.toDouble)).getOrElse(None),
      startDateTech = startDate,
      endDateTech = endDate,
      nCifreMis =  Try(Option(r.getAs[String](RcuGasTechSchema.n_num_cifre_misuratore)).map(_.toInt)).getOrElse(None)
    )
  }
}
