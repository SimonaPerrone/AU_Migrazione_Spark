package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasMassivoP
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasMassivoPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Viene utilizzata come perimetro base per le operazioni su rcugas;
 * contiene inoltre la relazione tra `t_codice_pdr` e `n_id_pdr`, utilizzata in alcune join all'interno del processo */
class RcuGasMassivoPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasMassivoPath
  override val columns: List[String] = List(
    RcuGasMassivoPSchema.t_codice_pdr,
    RcuGasMassivoPSchema.n_id_pdr,
    RcuGasMassivoPSchema.d_data_inizio_for,
    RcuGasMassivoPSchema.data_fine_for,
    RcuGasMassivoPSchema.t_trattamento,
    RcuGasMassivoPSchema.piva_udd,
    RcuGasMassivoPSchema.t_tipo_fornitura,
    RcuGasMassivoPSchema.n_id_fornitura,
    RcuGasMassivoPSchema.t_comune_istat_pdr,
    RcuGasMassivoPSchema.t_comune_istatforn
  )

  def get(): RDD[RcuGasMassivoP] = {
    readParquet
      .filter(col(RcuGasMassivoPSchema.n_id_fornitura).isNotNull)
      .rdd
      .map(mapFunc)
  }

  val mapFunc: Row => RcuGasMassivoP = (r:Row) => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
    RcuGasMassivoP(
      startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasMassivoPSchema.d_data_inizio_for)).toDateTimeAtStartOfDay) match {
        case Success(value) => value
        case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01 00:00:00.0").toDateTimeAtStartOfDay
      },
      endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasMassivoPSchema.data_fine_for)).toDateTimeAtStartOfDay) match {
        case Success(value) => value
        case Failure(_: NullPointerException) => formatter.parseDateTime("2999-12-31 23:59:59.0")
      },
      tCodicePdr = r.getAs[String](RcuGasMassivoPSchema.t_codice_pdr),
      nIdPdr = r.getAs[String](RcuGasMassivoPSchema.n_id_pdr),
      tTrattamento = Treatment.values.find(_.toString == r.getAs[String](RcuGasMassivoPSchema.t_trattamento)).getOrElse(Treatment.N),
      pivaUdd = Option(r.getAs[String](RcuGasMassivoPSchema.piva_udd)),
      tTipoFornitura = Option(r.getAs[String](RcuGasMassivoPSchema.t_tipo_fornitura)),
      tComuneIstatPdr = Option(r.getAs[String](RcuGasMassivoPSchema.t_comune_istat_pdr)),
      tComuneIstatForn = Option(r.getAs[String](RcuGasMassivoPSchema.t_comune_istatforn))
    )
  }
}
