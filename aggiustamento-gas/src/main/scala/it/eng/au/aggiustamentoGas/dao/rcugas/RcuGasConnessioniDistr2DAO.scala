package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasConnessioniDistr2
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasConnessioniDistr2Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, greatest, least}
import org.apache.spark.sql.types.StringType
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Contiene le informazioni sul distributore, codice remi e id regione climatica di un certo PdR */
class RcuGasConnessioniDistr2DAO extends Dao {
  override val parquetPath: String = Environment.getRcugasConnessioniDistr2RemiPath
  override val columns: List[String] = List(
    RcuGasConnessioniDistr2Schema.t_codice_pdr,
    RcuGasConnessioniDistr2Schema.n_id_distr,
    RcuGasConnessioniDistr2Schema.t_remi,
    RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
    RcuGasConnessioniDistr2Schema.d_data_fine_conn,
    RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
    RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione,
    RcuGasConnessioniDistr2Schema.id_regione_climatica
  )

  def get(startDate: String, endDate: String): RDD[RcuGasConnessioniDistr2] = {
    val readDf = readParquet
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, greatest(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), col(RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione)))
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, least(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), col(RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione)))

    filterDfWithStartEndDate(readDf
      , RcuGasConnessioniDistr2Schema.d_data_inizio_conn.toString
      , RcuGasConnessioniDistr2Schema.d_data_fine_conn.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startDate
      , endDate
      , "yyyyMM"
    )
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn).cast(StringType))
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, col(RcuGasConnessioniDistr2Schema.d_data_fine_conn).cast(StringType))
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasConnessioniDistr2 = (r: Row) => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    RcuGasConnessioniDistr2(
      dataInizioConn = Try(formatter.parseLocalDate(r.getAs[String](RcuGasConnessioniDistr2Schema.d_data_inizio_conn)).toDateTimeAtStartOfDay) match {
        case Success(value) => value
        case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
      },
      dataFineConn = Try(formatter.parseLocalDate(r.getAs[String](RcuGasConnessioniDistr2Schema.d_data_fine_conn)).toDateTimeAtStartOfDay) match {
        case Success(value) => value
        case Failure(_: NullPointerException) => formatter.parseDateTime("2100-12-31")
      },
      tCodicePdr = r.getAs[String](RcuGasConnessioniDistr2Schema.t_codice_pdr),
      nIdDistr = r.getAs[String](RcuGasConnessioniDistr2Schema.n_id_distr),
      tRemi = r.getAs[String](RcuGasConnessioniDistr2Schema.t_remi),
      idRegioneClimatica = Option(r.getAs[String](RcuGasConnessioniDistr2Schema.id_regione_climatica)).map(_.toInt)
    )
  }
}
