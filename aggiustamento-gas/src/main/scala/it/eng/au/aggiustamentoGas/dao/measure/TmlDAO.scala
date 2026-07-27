package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tml}
import it.eng.au.aggiustamentoGas.schema.measure.TmlSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class TmlDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getTmlParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    TmlSchema.cod_servizio,
    TmlSchema.cod_pdr,
    TmlSchema.data_racc,
    TmlSchema.let_tot_prel,
    TmlSchema.let_tot_conv,
    TmlSchema.matr_mis,
    TmlSchema.matr_conv,
    TmlSchema.tipo_lettura,
    TmlSchema.val_dato,
    TmlSchema.coeff_corr,
    TmlSchema.freq_let,
    TmlSchema.local_file,
    TmlSchema.d_caricamento,
    TmlSchema.piva_distr,
    TmlSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )
  /**
   * CR - Gabrini Federico - 16/12/2021 - delete tml "stimate" so tipo_lettura/readType = S
   */
  override val filterFlow: Flow => Boolean = (f:Flow) => (f.ammissibilita.isEmpty && f.isValid == Some("SI") && Set('E').contains(f.readType.getOrElse('-'))) ||
    (f.ammissibilita.isDefined && Set('E','A').contains(f.readType.getOrElse('-')))

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
      isValid = Option(r.getAs[String](TmlSchema.val_dato)),
      coefCorr = Try(Option(r.getAs[String](TmlSchema.coeff_corr)).map(_.toDouble)).getOrElse(None),
      freqLet = Try(Option(r.getAs[String](TmlSchema.freq_let)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](TmlSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TmlSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TmlSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TmlSchema.d_caricamento), dateLoadFormatter)
    )
  }
}
