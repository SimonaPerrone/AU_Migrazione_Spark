package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.dao.measure.TalDAO
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tal}
import it.eng.au.aggiustamentoGas.schema.measure.TalSchema
import org.apache.spark.sql.Row

import scala.util.Try

class TalDAOSbg extends TalDAO {
  override val columns: List[String] = List(
    TalSchema.cod_servizio,
    TalSchema.cod_pdr,
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

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.outcome == Some('v') || f.outcome == Some('V'))

  override val mapFunc: Row => Flow = (r: Row) => {
    var esito = r.getAs[String](TalSchema.esito_val)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    Tal(
      service = r.getAs[String](TalSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TalSchema.cod_pdr).toString,
      date = parseDateToOption(r.getAs[String](TalSchema.data_racc)),
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
