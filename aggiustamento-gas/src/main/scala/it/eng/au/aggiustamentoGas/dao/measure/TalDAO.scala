package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tal}
import it.eng.au.aggiustamentoGas.schema.measure.TalSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try


class TalDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getTalParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    TalSchema.cod_servizio,
    TalSchema.cod_pdr,
    TalSchema.data_com_autolet_cf,
    TalSchema.data_racc,
    TalSchema.let_tot_prel,
    TalSchema.let_tot_conv,
    TalSchema.matr_mis,
    TalSchema.matr_conv,
    TalSchema.local_file,
    TalSchema.d_caricamento,
    TalSchema.piva_distr,
    TalSchema.piva_utente,
    TalSchema.esito_val,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => f.ammissibilita.isDefined && (f.outcome == Some('v') || f.outcome == Some('V'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dataComAutoletCf = r.getAs[String](TalSchema.data_com_autolet_cf)
    val dataRacc = r.getAs[String](TalSchema.data_racc)
    var esito = r.getAs[String](TalSchema.esito_val)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    Tal(
      service = r.getAs[String](TalSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TalSchema.cod_pdr).toString,
      date = MeasureDAO.getDate(dataRacc, dataComAutoletCf),
      measure = Try(Option(r.getAs[String](TalSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](TalSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](TalSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TalSchema.matr_conv)),
      localFile = Option(r.getAs[String](TalSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TalSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TalSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TalSchema.d_caricamento), dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}
