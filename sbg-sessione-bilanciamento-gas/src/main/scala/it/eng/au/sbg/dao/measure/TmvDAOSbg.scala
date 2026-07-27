package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.ANNO_MESE_COL_NAME
import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, TmvDAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tmv}
import it.eng.au.aggiustamentoGas.schema.measure.TmvSchema
import org.apache.spark.sql.Row

import scala.util.Try

class TmvDAOSbg extends TmvDAO {
  override val columns: List[String] = List(
    TmvSchema.cod_servizio,
    TmvSchema.cod_pdr,
    TmvSchema.data_prest,
    TmvSchema.tipo_lettura,
    TmvSchema.let_tot_prel,
    TmvSchema.let_tot_conv,
    TmvSchema.matr_mis,
    TmvSchema.matr_conv,
    TmvSchema.local_file,
    TmvSchema.d_caricamento,
    TmvSchema.piva_distr,
    TmvSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {

    Tmv(
      service = r.getAs[String](TmvSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](TmvSchema.cod_pdr),
      readType = Try(Option(r.getAs[String](TmvSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](TmvSchema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](TmvSchema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](TmvSchema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](TmvSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TmvSchema.matr_conv)),
      localFile = Option(r.getAs[String](TmvSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TmvSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TmvSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](TmvSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
