package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasSuspendedPdr
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasFornituraPSchema, RcuGasSospensioniPSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Fornisce l'insieme dei PdR sospesi con i rispettivi periodi di sospensione */
class SuspendedPdrDAO extends Dao {

  val parquetPath: String = ""
  val columns: List[String] = List(
    RcuGasSospensioniPSchema.n_id_pdr,
    RcuGasSospensioniPSchema.d_data_inizio_sosp,
    RcuGasSospensioniPSchema.d_data_revoca_sosp
  )

  override def readParquet: DataFrame = {
    val rcuGasFornituraPDAO = new RcuGasFornituraPDAO
    val rcuGasSospensioniPDAO = new RcuGasSospensioniPDAO
    val rcuGasFornituraP = rcuGasFornituraPDAO.get()
    val rcuGasSospensioniP = rcuGasSospensioniPDAO.get()
    rcuGasFornituraP
      .join(rcuGasSospensioniP, col(RcuGasFornituraPSchema.n_id_fornitura) === col(RcuGasSospensioniPSchema.n_id_fornitura_sosp), "inner")
      .select(col(RcuGasSospensioniPSchema.n_id_pdr),
        col(RcuGasSospensioniPSchema.d_data_inizio_sosp).cast(StringType),
        col(RcuGasSospensioniPSchema.d_data_revoca_sosp).cast(StringType),
        col(RcuGasFornituraPSchema.d_data_fine).cast(StringType))
  }

  //already filter date with period start end date in rcuGasSospensioniPDAO.get()
  def get(): RDD[RcuGasSuspendedPdr] = {
    this.readParquet.rdd.map(r => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      RcuGasSuspendedPdr(
        nIdPdr = r.getAs[String](RcuGasSospensioniPSchema.n_id_pdr),
        dataIniSosp = Try(formatter.parseLocalDate(r.getAs[String](RcuGasSospensioniPSchema.d_data_inizio_sosp)).toDateTimeAtStartOfDay) match {
          case Success(value) => value
          case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
        },
        dataFineSosp = {
          val dataRevocaSosp = Try(formatter.parseLocalDate(r.getAs[String](RcuGasSospensioniPSchema.d_data_revoca_sosp)).toDateTimeAtStartOfDay) match {
            case Success(value) => value
            case Failure(_: NullPointerException) => formatter.parseLocalDate("2100-12-31").toDateTimeAtStartOfDay
          }
          val dataFineFornitura = Try(formatter.parseLocalDate(r.getAs[String](RcuGasFornituraPSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
            case Success(value) => value
            case Failure(_: NullPointerException) => formatter.parseLocalDate("2100-12-31").toDateTimeAtStartOfDay
          }
          if (dataRevocaSosp.isAfter(dataFineFornitura)) dataFineFornitura else dataRevocaSosp
        }
      )
    })
  }
}
