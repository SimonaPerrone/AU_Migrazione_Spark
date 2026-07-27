package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.ANNO_MESE_COL_NAME
import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, Sm1DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Sm1}
import it.eng.au.aggiustamentoGas.schema.measure.Sm1Schema
import org.apache.spark.sql.Row

import scala.util.Try

class Sm1DAOSbg extends Sm1DAO {
  override val columns: List[String] = List(
    Sm1Schema.cod_servizio,
    Sm1Schema.cod_pdr,
    Sm1Schema.data_ril,
    Sm1Schema.data_prest,
    Sm1Schema.tipo_lettura,
    Sm1Schema.segn_mis,
    Sm1Schema.segn_conv,
    Sm1Schema.let_tot_prel,
    Sm1Schema.let_tot_conv,
    Sm1Schema.matr_mis,
    Sm1Schema.matr_conv,
    Sm1Schema.local_file,
    Sm1Schema.d_caricamento,
    Sm1Schema.piva_distr,
    Sm1Schema.piva_utente,
    Sm1Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Sm1(
      service = r.getAs[String](Sm1Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](Sm1Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](Sm1Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](Sm1Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](Sm1Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](Sm1Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](Sm1Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Sm1Schema.matr_conv)),
      localFile = Option(r.getAs[String](Sm1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Sm1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Sm1Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Sm1Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
