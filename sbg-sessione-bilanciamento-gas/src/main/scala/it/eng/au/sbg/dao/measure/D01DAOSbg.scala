package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{D01DAO, MeasureDAO}
import it.eng.au.aggiustamentoGas.model.measure.{D01, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.D01Schema
import org.apache.spark.sql.Row

import scala.util.Try

class D01DAOSbg extends D01DAO {
  override val columns: List[String] = List(
    D01Schema.cod_servizio,
    D01Schema.cod_pdr,
    D01Schema.data_prest,
    D01Schema.tipo_lettura,
    D01Schema.segn_mis,
    D01Schema.segn_conv,
    D01Schema.let_tot_prel,
    D01Schema.let_tot_conv,
    D01Schema.matr_mis,
    D01Schema.matr_conv,
    D01Schema.local_file,
    D01Schema.d_caricamento,
    D01Schema.piva_distr,
    D01Schema.piva_utente,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {

    D01(
      service = r.getAs[String](D01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](D01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](D01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](D01Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](D01Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](D01Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](D01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](D01Schema.matr_conv)),
      localFile = Option(r.getAs[String](D01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](D01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](D01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](D01Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
