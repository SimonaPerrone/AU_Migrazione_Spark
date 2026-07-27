package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.dao.measure.TmlDAO
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tml}
import it.eng.au.aggiustamentoGas.schema.measure.TmlSchema
import org.apache.spark.sql.Row

import scala.util.Try

class TmlDAOSbg extends TmlDAO {
  override val columns: List[String] = List(
    TmlSchema.cod_servizio,
    TmlSchema.cod_pdr,
    TmlSchema.data_racc,
    TmlSchema.let_tot_prel,
    TmlSchema.let_tot_conv,
    TmlSchema.matr_mis,
    TmlSchema.matr_conv,
    TmlSchema.tipo_lettura,
    TmlSchema.coeff_corr,
    TmlSchema.freq_let,
    TmlSchema.local_file,
    TmlSchema.d_caricamento,
    TmlSchema.piva_distr,
    TmlSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Tml(
      service = r.getAs[String](TmlSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TmlSchema.cod_pdr).toString,
      readType = Try(Option(r.getAs[String](TmlSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = parseDateToOption(r.getAs[String](TmlSchema.data_racc)),
      measure = Try(Option(r.getAs[String](TmlSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](TmlSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](TmlSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TmlSchema.matr_conv)),
      coefCorr = Try(Option(r.getAs[String](TmlSchema.coeff_corr)).map(_.toDouble)).getOrElse(None),
      freqLet = Try(Option(r.getAs[String](TmlSchema.freq_let)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](TmlSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TmlSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TmlSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TmlSchema.d_caricamento), dateLoadFormatter)
    )
  }
}
