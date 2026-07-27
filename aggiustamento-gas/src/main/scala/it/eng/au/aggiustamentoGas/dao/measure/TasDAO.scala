package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tas}
import it.eng.au.aggiustamentoGas.schema.measure.TasSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class TasDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getTasParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    TasSchema.cod_servizio,
    TasSchema.cod_pdr,
    TasSchema.data_com_autolet_cf,
    TasSchema.data_racc,
    TasSchema.let_tot_prel,
    TasSchema.let_tot_conv,
    TasSchema.matr_mis,
    TasSchema.matr_conv,
    TasSchema.local_file,
    TasSchema.d_caricamento,
    TasSchema.piva_distr,
    TasSchema.piva_utente,
    TasSchema.esito_val,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => f.ammissibilita.isDefined && (f.outcome == Some('v') || f.outcome == Some('V'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dataComAutoletCf = r.getAs[String](TasSchema.data_com_autolet_cf)
    val dataRacc = r.getAs[String](TasSchema.data_racc)
    var esito = r.getAs[String](TasSchema.esito_val)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    Tas(
      service = r.getAs[String](TasSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TasSchema.cod_pdr).toString,
      date = MeasureDAO.getDate(dataRacc, dataComAutoletCf),
      measure = Try(Option(r.getAs[String](TasSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](TasSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](TasSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TasSchema.matr_conv)),
      localFile = Option(r.getAs[String](TasSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TasSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TasSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TasSchema.d_caricamento), dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}
