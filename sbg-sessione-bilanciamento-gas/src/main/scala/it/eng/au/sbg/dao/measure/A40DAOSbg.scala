package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{A40DAO, MeasureDAO}
import it.eng.au.aggiustamentoGas.model.measure.{A40, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A40Schema
import org.apache.spark.sql.Row

import scala.util.Try

class A40DAOSbg extends A40DAO {
  override val columns: List[String] = List(
    A40Schema.cod_servizio,
    A40Schema.cod_pdr,
    A40Schema.data_prest,
    A40Schema.tipo_lettura,
    A40Schema.segn_mis,
    A40Schema.segn_conv,
    A40Schema.let_tot_prel,
    A40Schema.let_tot_conv,
    A40Schema.matr_mis,
    A40Schema.matr_conv,
    A40Schema.local_file,
    A40Schema.d_caricamento,
    A40Schema.piva_distr,
    A40Schema.piva_utente,
    A40Schema.esito,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {

    A40(
      service = r.getAs[String](A40Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](A40Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](A40Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](A40Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](A40Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](A40Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](A40Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A40Schema.matr_conv)),
      localFile = Option(r.getAs[String](A40Schema.local_file)),
      pivaDistr = Option(r.getAs[String](A40Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A40Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A40Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
