package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, R01DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, R01}
import it.eng.au.aggiustamentoGas.schema.measure.R01Schema
import org.apache.spark.sql.Row

import scala.util.Try

class R01DAOSbg extends R01DAO {
  override val columns: List[String] = List(
    R01Schema.cod_servizio,
    R01Schema.cod_pdr,
    R01Schema.data_prest,
    R01Schema.tipo_lettura,
    R01Schema.segn_mis,
    R01Schema.segn_conv,
    R01Schema.let_tot_prel,
    R01Schema.let_tot_conv,
    R01Schema.matr_mis,
    R01Schema.matr_conv,
    R01Schema.local_file,
    R01Schema.d_caricamento,
    R01Schema.piva_distr,
    R01Schema.piva_utente,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    R01(
      service = r.getAs[String](R01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](R01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](R01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](R01Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](R01Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](R01Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](R01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](R01Schema.matr_conv)),
      localFile = Option(r.getAs[String](R01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](R01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](R01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](R01Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
