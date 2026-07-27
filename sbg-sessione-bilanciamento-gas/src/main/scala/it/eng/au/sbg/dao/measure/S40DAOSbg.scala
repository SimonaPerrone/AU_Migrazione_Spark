package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, S40DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, S40}
import it.eng.au.aggiustamentoGas.schema.measure.S40Schema
import org.apache.spark.sql.Row

import scala.util.Try

class S40DAOSbg extends S40DAO {
  override val columns: List[String] = List(
    S40Schema.cod_flusso,
    S40Schema.cod_pdr,
    S40Schema.data_prest,
    S40Schema.let_tot_prel,
    S40Schema.let_tot_conv,
    S40Schema.matr_mis,
    S40Schema.matr_conv,
    S40Schema.local_file,
    S40Schema.d_caricamento,
    S40Schema.piva_distr,
    S40Schema.piva_utente,
    S40Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    S40(
      service = r.getAs[String](S40Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](S40Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](S40Schema.data_prest)),
      readType = Try(Option(r.getAs[String](S40Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](S40Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](S40Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](S40Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](S40Schema.matr_conv)),
      localFile = Option(r.getAs[String](S40Schema.local_file)),
      pivaDistr = Option(r.getAs[String](S40Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](S40Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](S40Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
