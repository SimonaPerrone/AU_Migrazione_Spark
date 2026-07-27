package it.eng.au.aggiustamentoGas.dao.measure
import it.eng.au.aggiustamentoGas.controller.CoefficientController
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmr, IgmrPost, IgmrPre}
import it.eng.au.aggiustamentoGas.schema.measure.IgmrSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class IgmrDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getIgmrParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    IgmrSchema.cod_flusso,
    IgmrSchema.cod_pdr,
    IgmrSchema.data_misura,
    IgmrSchema.let_misuratore_pre_int,
    IgmrSchema.let_correttore_pre_int,
    IgmrSchema.matr_mis_pre_int,
    IgmrSchema.matr_conv_pre_int,
    IgmrSchema.mot_ret_lett,
    IgmrSchema.coeff_corr_pre_int,
    IgmrSchema.let_misuratore_post_int,
    IgmrSchema.let_correttore_post_int,
    IgmrSchema.matr_mis_post_int,
    IgmrSchema.matr_conv_post_int,
    IgmrSchema.coeff_corr_post_int,
    IgmrSchema.local_file,
    IgmrSchema.cau_int_mis,
    IgmrSchema.cau_int_cor,
    IgmrSchema.d_caricamento,
    IgmrSchema.piva_distr,
    IgmrSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => true

  override val mapFunc: Row => Flow = (r: Row) => {
    val igmrPre = IgmrPre(
      service = r.getAs[String](IgmrSchema.cod_flusso).toUpperCase + "PRE",
      pdr = r.getAs[String](IgmrSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmrSchema.data_misura)),
      measure = Try(Option(r.getAs[String](IgmrSchema.let_misuratore_pre_int)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](IgmrSchema.let_correttore_pre_int)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](IgmrSchema.matr_mis_pre_int)),
      serialNumberConv = Option(r.getAs[String](IgmrSchema.matr_conv_pre_int)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](IgmrSchema.coeff_corr_pre_int)).map(_.toDouble)).getOrElse(None)),
      motivation = Try(Option(r.getAs[String](IgmrSchema.mot_ret_lett)).map(_.toInt)).getOrElse(None),
      cau_int_mis = Try(Option(r.getAs[String](IgmrSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmrSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmrSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmrSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmrSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmrSchema.d_caricamento), dateLoadFormatter)
    )

    val igmrPost = IgmrPost(
      service = r.getAs[String](IgmrSchema.cod_flusso).toUpperCase + "POST",
      pdr = r.getAs[String](IgmrSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmrSchema.data_misura)),
      measure = Try(Option(r.getAs[String](IgmrSchema.let_misuratore_post_int)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](IgmrSchema.let_correttore_post_int)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](IgmrSchema.matr_mis_post_int)),
      serialNumberConv = Option(r.getAs[String](IgmrSchema.matr_conv_post_int)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](IgmrSchema.coeff_corr_post_int)).map(_.toDouble)).getOrElse(None)),
      motivation = Try(Option(r.getAs[String](IgmrSchema.mot_ret_lett)).map(_.toInt)).getOrElse(None),
      cau_int_mis = Try(Option(r.getAs[String](IgmrSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmrSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmrSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmrSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmrSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmrSchema.d_caricamento), dateLoadFormatter)
    )

    Igmr(
      service = r.getAs[String](IgmrSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](IgmrSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmrSchema.data_misura)),
      motivation = Try(Option(r.getAs[String](IgmrSchema.mot_ret_lett)).map(_.toInt)).getOrElse(None),
      cau_int_mis = Try(Option(r.getAs[String](IgmrSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmrSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmrSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmrSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmrSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmrSchema.d_caricamento), dateLoadFormatter),
      pre = igmrPre,
      post = igmrPost
    )
  }
}
