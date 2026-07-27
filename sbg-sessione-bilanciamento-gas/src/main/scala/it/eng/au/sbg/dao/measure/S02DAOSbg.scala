package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, S02DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, S02}
import it.eng.au.aggiustamentoGas.schema.measure.S02Schema
import org.apache.spark.sql.Row

import scala.util.Try

class S02DAOSbg extends S02DAO {
  override val columns: List[String] = List(
    S02Schema.cod_flusso,
    S02Schema.cod_pdr,
    S02Schema.data_prest,
    S02Schema.let_tot_prel,
    S02Schema.let_tot_conv,
    S02Schema.matr_mis,
    S02Schema.matr_conv,
    S02Schema.local_file,
    S02Schema.d_caricamento,
    S02Schema.piva_distr,
    S02Schema.piva_utente,
    S02Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    S02(
      service = r.getAs[String](S02Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](S02Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](S02Schema.data_prest)),
      readType = Try(Option(r.getAs[String](S02Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](S02Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](S02Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](S02Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](S02Schema.matr_conv)),
      localFile = Option(r.getAs[String](S02Schema.local_file)),
      pivaDistr = Option(r.getAs[String](S02Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](S02Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](S02Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
