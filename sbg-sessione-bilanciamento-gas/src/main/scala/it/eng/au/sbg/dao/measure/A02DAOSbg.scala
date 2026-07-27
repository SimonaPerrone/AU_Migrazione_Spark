package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{A02DAO, MeasureDAO}
import it.eng.au.aggiustamentoGas.model.measure.{A02, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A01Schema
import org.apache.spark.sql.Row

import scala.util.Try

class A02DAOSbg extends A02DAO {
  override val columns: List[String] = List(
    A01Schema.cod_servizio,
    A01Schema.cod_pdr,
    A01Schema.data_prest,
    A01Schema.tipo_lettura,
    A01Schema.segn_mis,
    A01Schema.segn_conv,
    A01Schema.let_tot_prel,
    A01Schema.let_tot_conv,
    A01Schema.matr_mis,
    A01Schema.matr_conv,
    A01Schema.local_file,
    A01Schema.d_caricamento,
    A01Schema.piva_distr,
    A01Schema.piva_utente,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    A02(
      service = r.getAs[String](A01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](A01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](A01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](A01Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](A01Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](A01Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](A01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A01Schema.matr_conv)),
      localFile = Option(r.getAs[String](A01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](A01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A01Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
