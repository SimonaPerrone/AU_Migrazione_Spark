package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.controller.CoefficientController
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Im1, Im1Post, Im1Pre}
import it.eng.au.aggiustamentoGas.schema.measure.Im1Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Im1DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getIm1ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    Im1Schema.cod_servizio,
    Im1Schema.cod_pdr,
    Im1Schema.data_esec_int,
    Im1Schema.PRE_let_misuratore,
    Im1Schema.PRE_let_correttore,
    Im1Schema.PRE_matr_mis,
    Im1Schema.PRE_matr_conv,
    Im1Schema.PRE_tipo_mis,
    Im1Schema.PRE_coeff_corr,
    Im1Schema.POST_let_misuratore,
    Im1Schema.POST_let_correttore,
    Im1Schema.POST_matr_mis,
    Im1Schema.POST_matr_conv,
    Im1Schema.POST_coeff_corr,
    Im1Schema.local_file,
    Im1Schema.cau_int_mis,
    Im1Schema.cau_int_cor,
    Im1Schema.d_caricamento,
    Im1Schema.piva_distr,
    Im1Schema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => f.ammissibilita.isDefined || Set('E', 'S').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val im1Pre = Im1Pre(
      service = r.getAs[String](Im1Schema.cod_servizio).toUpperCase + "PRE",
      pdr = r.getAs[String](Im1Schema.cod_pdr),
      date = parseDateToOption(r.getAs[String](Im1Schema.data_esec_int)),
      readType = Try(Option(r.getAs[String](Im1Schema.PRE_tipo_mis)).map(_ (0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Im1Schema.PRE_let_misuratore)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Im1Schema.PRE_let_correttore)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Im1Schema.PRE_matr_mis)),
      serialNumberConv = Option(r.getAs[String](Im1Schema.PRE_matr_conv)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](Im1Schema.PRE_coeff_corr)).map(_.toDouble)).getOrElse(None)),
      cau_int_mis = Try(Option(r.getAs[String](Im1Schema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](Im1Schema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](Im1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Im1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Im1Schema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](Im1Schema.d_caricamento), dateLoadFormatter)
    )

    val im1Post = Im1Post(
      service = r.getAs[String](Im1Schema.cod_servizio).toUpperCase + "POST",
      pdr = r.getAs[String](Im1Schema.cod_pdr),
      date = parseDateToOption(r.getAs[String](Im1Schema.data_esec_int)),
      readType = Try(Option(r.getAs[String](Im1Schema.PRE_tipo_mis)).map(_ (0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Im1Schema.POST_let_misuratore)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Im1Schema.POST_let_correttore)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Im1Schema.POST_matr_mis)),
      serialNumberConv = Option(r.getAs[String](Im1Schema.POST_matr_conv)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](Im1Schema.POST_coeff_corr)).map(_.toDouble)).getOrElse(None)),
      cau_int_mis = Try(Option(r.getAs[String](Im1Schema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](Im1Schema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](Im1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Im1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Im1Schema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](Im1Schema.d_caricamento), dateLoadFormatter)
    )

    Im1(
      service = r.getAs[String](Im1Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](Im1Schema.cod_pdr),
      date = parseDateToOption(r.getAs[String](Im1Schema.data_esec_int)),
      readType = Try(Option(r.getAs[String](Im1Schema.PRE_tipo_mis)).map(_ (0))).getOrElse(None),
      cau_int_mis = Try(Option(r.getAs[String](Im1Schema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](Im1Schema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](Im1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Im1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Im1Schema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](Im1Schema.d_caricamento), dateLoadFormatter),
      pre = im1Pre,
      post = im1Post
    )
  }
}
