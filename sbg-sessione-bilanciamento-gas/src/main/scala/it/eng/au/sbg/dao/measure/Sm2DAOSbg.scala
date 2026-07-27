package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.ANNO_MESE_COL_NAME
import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, Sm2DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Sm2}
import it.eng.au.aggiustamentoGas.schema.measure.Sm2Schema
import org.apache.spark.sql.Row

import scala.util.Try

class Sm2DAOSbg extends Sm2DAO {
  override val columns: List[String] = List(
    Sm2Schema.cod_servizio,
    Sm2Schema.cod_pdr,
    Sm2Schema.data_ril,
    Sm2Schema.data_prest,
    Sm2Schema.tipo_lettura,
    Sm2Schema.segn_mis,
    Sm2Schema.segn_conv,
    Sm2Schema.let_tot_prel,
    Sm2Schema.let_tot_conv,
    Sm2Schema.matr_mis,
    Sm2Schema.matr_conv,
    Sm2Schema.local_file,
    Sm2Schema.d_caricamento,
    Sm2Schema.piva_distr,
    Sm2Schema.piva_utente,
    Sm2Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Sm2(
      service = r.getAs[String](Sm2Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](Sm2Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](Sm2Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](Sm2Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](Sm2Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](Sm2Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](Sm2Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Sm2Schema.matr_conv)),
      localFile = Option(r.getAs[String](Sm2Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Sm2Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Sm2Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Sm2Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
