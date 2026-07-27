package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.dao.measure.TavDAO
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tav}
import it.eng.au.aggiustamentoGas.schema.measure.{TalSchema, TasSchema, TavSchema}
import org.apache.spark.sql.Row

import scala.util.Try

class TavDAOSbg extends TavDAO {
  override val columns: List[String] = List(
    TavSchema.cod_servizio,
    TavSchema.cod_pdr,
    TavSchema.data_racc,
    TavSchema.let_tot_prel,
    TavSchema.let_tot_conv,
    TavSchema.matr_mis,
    TavSchema.matr_conv,
    TavSchema.local_file,
    TavSchema.d_caricamento,
    TavSchema.piva_distr,
    TavSchema.piva_utente,
    TasSchema.esito_val,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.outcome == Some('v') || f.outcome == Some('V'))

  override val mapFunc: Row => Flow = (r: Row) => {
    var esito = r.getAs[String](TalSchema.esito_val)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    Tav(
      service = r.getAs[String](TavSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TavSchema.cod_pdr).toString,
      date = parseDateToOption(r.getAs[String](TavSchema.data_racc)),
      measure = Try(Option(r.getAs[String](TavSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](TavSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](TavSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TavSchema.matr_conv)),
      localFile = Option(r.getAs[String](TavSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TavSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TavSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TavSchema.d_caricamento), dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}
